/*
 * Copyright 2015 udeyoje.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.strategicgains.docussandra.controller.perf.remote;

import com.jayway.restassured.RestAssured;
import static com.jayway.restassured.RestAssured.expect;
import static com.jayway.restassured.RestAssured.given;
import com.jayway.restassured.response.Response;
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import testhelper.RestExpressManager;

/**
 * Treat this as a singleton, even though it is not.
 *
 * @author udeyoje
 */
public class PlayersRemote
{

    private final static Logger logger = LoggerFactory.getLogger(PlayersRemote.class);
    private final static Logger output = LoggerFactory.getLogger("output");
    private static final String BASE_URI = "http://localhost";//"https://docussandra.stg-prsn.com";
    private static final int PORT = 19080;

    private static final int NUM_WORKERS = 50; //NOTE: one more worker will be added to pick up any remainder

    private static AtomicInteger errorCount = new AtomicInteger(0);

    private static AtomicLong tft = new AtomicLong(0);

    public PlayersRemote() throws IOException
    {
        RestExpressManager.getManager().ensureRestExpressRunning();
        RestAssured.baseURI = BASE_URI;
        RestAssured.port = PORT;
        RestAssured.basePath = "/";
        RestAssured.useRelaxedHTTPSValidation();

    }

    @Test
    public void loadData() throws IOException, ParseException, InterruptedException
    {
        List<Document> docs = getDocumentsFromFS();
        int numDocs = docs.size();
        int docsPerWorker = numDocs / NUM_WORKERS;
        int numDocsAssigned = 0;
        ArrayList<List<Document>> documentQueues = new ArrayList<>(NUM_WORKERS + 1);
        while ((numDocsAssigned + 1) < numDocs)
        {
            int start = numDocsAssigned;
            int end = numDocsAssigned + docsPerWorker;
            if (end > numDocs)
            {
                end = numDocs - 1;
            }
            documentQueues.add(new ArrayList(docs.subList(start, end)));
            numDocsAssigned = end;
        }

        ArrayList<Thread> workers = new ArrayList<>(NUM_WORKERS + 1);
        for (final List<Document> queue : documentQueues)
        {
            workers.add(new Thread()
            {
                @Override
                public void run()
                {
                    for (Document d : queue)
                    {
                        postDocument(getDb(), getTb(), d);
                    }
                    logger.info("Thread " + Thread.currentThread().getName() + " is done.");
                }
            });
        }
        long start = new Date().getTime();
        //start your threads!
        for (Thread t : workers)
        {
            t.start();
        }
        logger.info("All threads started, waiting for completion.");
        boolean allDone = false;
        boolean first = true;
        while (!allDone || first)
        {
            first = false;
            boolean done = true;
            for (Thread t : workers)
            {
                if (t.isAlive())
                {
                    done = false;
                    logger.info("Thread " + t.getName() + " is still running.");
                    break;
                }
            }
            if (done)
            {
                allDone = true;
            } else
            {
                logger.info("We still have workers running...");
                Thread.sleep(10000);
            }
        }
        long end = new Date().getTime();
        long miliseconds = (end - start);
        double seconds = (double) miliseconds / 1000d;
        output.info("Done loading data using: " + NUM_WORKERS + " and URL: " + BASE_URI + ". Took: " + seconds + " seconds");
        double tpms = ((double) numDocs / (double) miliseconds);
        double tps = tpms * 1000;
        double transactionTime = ((double) tft.get() / (double) numDocs);
        output.info("Average Transactions Per Second: " + tps);
        output.info("Average Transactions Time (in miliseconds): " + transactionTime);

    }

    @Before
    public void beforeTest() throws Exception
    {
        deleteData(getDb(), getTb(), getIndexes());//should delete everything related to this table
        postDB(getDb());
        postTable(getDb(), getTb());
        for (Index i : getIndexes())
        {
            postIndex(getDb(), getTb(), i);
        }
    }

    @After
    public void afterTest() throws InterruptedException
    {
        deleteData(getDb(), getTb(), getIndexes());//should delete everything related to this table
    }

    @AfterClass
    public static void afterClass() throws InterruptedException
    {
        Thread.sleep(10000);//have to let the deletes finish before shutting down
    }
    
    private List<Document> getDocumentsFromFS() throws IOException, ParseException{
        return Fixtures.getBulkDocuments("./src/test/resources/players.json", getTb());
    }

    private static void postDB(Database database)
    {
        logger.debug("Creating test DB");
        String dbStr = "{" + "\"description\" : \"" + database.description()
                + "\"," + "\"name\" : \"" + database.name() + "\"}";
        //act
        given().body(dbStr).expect().statusCode(201)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("name", equalTo(database.name()))
                .body("description", equalTo(database.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post(database.name());
        //check
        expect().statusCode(200)
                .body("name", equalTo(database.name()))
                .body("description", equalTo(database.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when()
                .get("/" + database.getId());
    }

    private static void deleteData(Database database, Table table, List<Index> index)
    {
        logger.debug("Deleteing test DB");
        //act
        given().when().delete(database.name());
        given().when().delete(database.name() + "/" + table.name());
        for (Index i : index)
        {
            given().when().delete(database.name() + "/" + table.name() + "/indexes/" + i.name());
        }
    }

    private void postTable(Database database, Table table)
    {
        logger.debug("Creating test table");
        String tableStr = "{" + "\"description\" : \"" + table.description()
                + "\"," + "\"name\" : \"" + table.name() + "\"}";
        //act
        given().body(tableStr).expect().statusCode(201)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("name", equalTo(table.name()))
                .body("description", equalTo(table.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post(database.name() + "/" + table.name());
        //check
        expect().statusCode(200)
                .body("name", equalTo(table.name()))
                .body("description", equalTo(table.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(database.name() + "/" + table.name());
    }

//    private void deleteTable()
//    {
//        logger.debug("Deleteing test table");
//        given().expect().statusCode(204)
//                .when().delete(tb.name());
//        //check
//        expect().statusCode(404).when()
//                .get(tb.name());
//    }
    private void postIndex(Database database, Table table, Index index)
    {
        logger.info("POSTing index: " + index.toString());
        boolean first = true;
        StringBuilder tableStr = new StringBuilder("{" + "\"fields\" : [");
        for (String field : index.fields())
        {
            if (!first)
            {
                tableStr.append(", ");
            } else
            {
                first = false;
            }
            tableStr.append("\"");
            tableStr.append(field);
            tableStr.append("\"");
        }
        tableStr.append("],").append("\"name\" : \"").append(index.name()).append("\"}");

        //act
        given().body(tableStr.toString()).expect().statusCode(201)
                .body("name", equalTo(index.name()))
                .body("fields", notNullValue())
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post(database.name() + "/" + table.name() + "/indexes/" + index.name());

        //check
        expect().statusCode(200)
                .body("name", equalTo(index.name()))
                .body("fields", notNullValue())
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .get(database.name() + "/" + table.name() + "/indexes/" + index.name());
    }

    private static void postDocument(Database database, Table table, Document d)
    {
        //act
        long start = new Date().getTime();
        Response response = given().body(d.object()).expect().when().post(database.name() + "/" + table.name() + "/").andReturn();
        long end = new Date().getTime();
        long tftt = end - start;
        tft.addAndGet(tftt);
        int code = response.getStatusCode();
        if (code != 201)
        {
            errorCount.addAndGet(1);
            logger.info("Error publishing document: " + response.getBody().prettyPrint());
            logger.info("This is the: " + errorCount.toString() + " error.");
        }
    }

    /**
     * @return the db
     */
    public Database getDb()
    {
        Database db = new Database("players");
        db.description("A database about players.");
        return db;
    }

    /**
     * @return the tb
     */
    public Table getTb()
    {
        Table tb = new Table();
        tb.name("players_table");
        tb.description("A table about players.");
        return tb;
    }

    /**
     * @return the indexes
     */
    public List<Index> getIndexes()
    {
        ArrayList<Index> indexes = new ArrayList<>(6);
        Index player = new Index("player");
        player.isUnique(false);
        List<String> fields = new ArrayList<>(1);
        fields.add("NAMEFULL");
        player.fields(fields);
        player.table(getTb());

        Index lastname = new Index("lastname");
        lastname.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("NAMELAST");
        lastname.fields(fields);
        lastname.table(getTb());

        Index lastAndFirst = new Index("lastandfirst");
        lastAndFirst.isUnique(false);
        fields = new ArrayList<>(2);
        fields.add("NAMELAST");
        fields.add("NAMEFIRST");
        lastAndFirst.fields(fields);
        lastAndFirst.table(getTb());

        Index team = new Index("team");
        team.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("TEAM");
        team.fields(fields);
        team.table(getTb());

        Index position = new Index("postion");
        position.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("POSITION");
        position.fields(fields);
        position.table(getTb());

        Index rookie = new Index("rookieyear");
        rookie.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("ROOKIEYEAR");
        rookie.fields(fields);
        rookie.table(getTb());

        indexes.add(team);
        indexes.add(position);
        indexes.add(player);
        indexes.add(rookie);
        indexes.add(lastAndFirst);
        indexes.add(lastname);
        return indexes;
    }

}

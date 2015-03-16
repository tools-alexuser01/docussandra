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
    private static final String BASE_URI = "http://localhost";//"https://docussandra.stg-prsn.com";
    private static final int PORT = 19080;

    private static Database playersDb;
    private static Table playersTable;
    private static List<Index> indexes = new ArrayList<>();

    private static final int NUM_WORKERS = 50; //NOTE: one more worker will be added to pick up any remainder

    private static AtomicInteger errorCount = new AtomicInteger(0);

    private static AtomicLong tft = new AtomicLong(0);
    private static int t = 0;

    public PlayersRemote() throws IOException
    {
        RestExpressManager.getManager().ensureRestExpressRunning();
        RestAssured.baseURI = BASE_URI;
        RestAssured.port = PORT;
        RestAssured.basePath = "/";
        RestAssured.useRelaxedHTTPSValidation();

        playersDb = new Database("players");
        playersDb.description("A database about players.");

        playersTable = new Table();
        playersTable.name("players_table");
        playersTable.description("A table about players.");

        Index player = new Index("player");
        player.isUnique(false);
        List<String> fields = new ArrayList<>(1);
        fields.add("NAMEFULL");
        player.fields(fields);
        player.table(playersTable);

        Index lastname = new Index("lastname");
        lastname.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("NAMELAST");
        lastname.fields(fields);
        lastname.table(playersTable);

        Index lastAndFirst = new Index("lastandfirst");
        lastAndFirst.isUnique(false);
        fields = new ArrayList<>(2);
        fields.add("NAMELAST");
        fields.add("NAMEFIRST");
        lastAndFirst.fields(fields);
        lastAndFirst.table(playersTable);

        Index team = new Index("team");
        team.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("TEAM");
        team.fields(fields);
        team.table(playersTable);

        Index position = new Index("postion");
        position.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("POSITION");
        position.fields(fields);
        position.table(playersTable);

        Index rookie = new Index("rookieyear");
        rookie.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("ROOKIEYEAR");
        rookie.fields(fields);
        rookie.table(playersTable);

        indexes.add(team);
        indexes.add(position);
        indexes.add(player);
        indexes.add(rookie);
        indexes.add(lastAndFirst);
        indexes.add(lastname);

    }

    @Test
    public void loadData() throws IOException, ParseException, InterruptedException
    {
        List<Document> docs = Fixtures.getBulkDocuments("./src/test/resources/players.json", playersTable);
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
                        postDocument(d);
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
        long seconds = miliseconds / 1000;
        logger.info("Done loading data! Took: " + seconds + " seconds");
        double tpms = ((double)numDocs / (double)miliseconds);
        double tps = tpms * 1000;
        double transactionTime = ((double)tft.get() / (double)numDocs);
        logger.info("Average Transactions Per Second: " + tps);
        logger.info("Average Transactions Time (in miliseconds): " + transactionTime);
        
    }

    @Before
    public void beforeTest() throws Exception
    {
        deleteData();//should delete everything related to this table
        postDB();
        postTable();
        for (Index i : indexes)
        {
            postIndex(i);
        }
    }

    @After
    public void afterTest() throws InterruptedException
    {
        deleteData();
    }

    @AfterClass
    public static void afterClass() throws InterruptedException
    {
        deleteData();
        Thread.sleep(10000);//have to let the deletes finish before shutting down
    }

    private void postDB()
    {
        logger.debug("Creating test DB");
        String dbStr = "{" + "\"description\" : \"" + playersDb.description()
                + "\"," + "\"name\" : \"" + playersDb.name() + "\"}";
        //act
        given().body(dbStr).expect().statusCode(201)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("name", equalTo(playersDb.name()))
                .body("description", equalTo(playersDb.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post(playersDb.name());
        //check
        expect().statusCode(200)
                .body("name", equalTo(playersDb.name()))
                .body("description", equalTo(playersDb.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when()
                .get("/" + playersDb.getId());
    }

    private static void deleteData()
    {
        logger.debug("Deleteing test DB");
        //act
        given().when().delete(playersDb.name());
        given().when().delete(playersDb.name() + "/" + playersTable.name());
        for (Index i : indexes)
        {
            given().when().delete(playersDb.name() + "/" + playersTable.name() + "/indexes/" + i.name());
        }
    }

    private void postTable()
    {
        logger.debug("Creating test table");
        String tableStr = "{" + "\"description\" : \"" + playersTable.description()
                + "\"," + "\"name\" : \"" + playersTable.name() + "\"}";
        //act
        given().body(tableStr).expect().statusCode(201)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("name", equalTo(playersTable.name()))
                .body("description", equalTo(playersTable.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post(playersDb.name() + "/" + playersTable.name());
        //check
        expect().statusCode(200)
                .body("name", equalTo(playersTable.name()))
                .body("description", equalTo(playersTable.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(playersDb.name() + "/" + playersTable.name());
    }

//    private void deleteTable()
//    {
//        logger.debug("Deleteing test table");
//        given().expect().statusCode(204)
//                .when().delete(playersTable.name());
//        //check
//        expect().statusCode(404).when()
//                .get(playersTable.name());
//    }
    private void postIndex(Index index)
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
                .when().post(playersDb.name() + "/" + playersTable.name() + "/indexes/" + index.name());

        //check
        expect().statusCode(200)
                .body("name", equalTo(index.name()))
                .body("fields", notNullValue())
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .get(playersDb.name() + "/" + playersTable.name() + "/indexes/" + index.name());
    }

    private static void postDocument(Document d)
    {
        //act
        long start = new Date().getTime();
        Response response = given().body(d.object()).expect().when().post(playersDb.name() + "/" + playersTable.name() + "/").andReturn();
        long end = new Date().getTime();
        long tftt = end - start;
        tft.addAndGet(tftt);
        t++;
        int code = response.getStatusCode();
        if (code != 201)
        {
            errorCount.addAndGet(1);
            logger.info("Error publishing document: " + response.getBody().prettyPrint());
            logger.info("This is the: " + errorCount.toString() + " error.");
        }
    }

}

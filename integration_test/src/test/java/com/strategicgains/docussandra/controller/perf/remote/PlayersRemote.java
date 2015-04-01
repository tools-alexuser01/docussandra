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

import static com.jayway.restassured.RestAssured.given;
import com.strategicgains.docussandra.controller.perf.remote.parent.PerfTestParent;
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.domain.QueryResponseWrapper;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.persistence.QueryRepository;
import com.strategicgains.docussandra.service.QueryService;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import static org.hamcrest.Matchers.notNullValue;
import org.json.simple.parser.ParseException;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Does a perf test using the players data.
 *
 * @author udeyoje
 */
public class PlayersRemote extends PerfTestParent
{

    private static boolean setupRun = false;

    public PlayersRemote() throws IOException, InterruptedException, ParseException
    {
        if (!setupRun)
        {
            setupRun = true;
            setup();
        }
    }

    protected void setup() throws IOException, InterruptedException, ParseException
    {
        logger.info("Setup called!");
        beforeClass();
        //deleteData(getDb(), getTb(), getIndexes()); //should delete everything related to this table
        postDB(getDb());
        postTable(getDb(), getTb());
        for (Index i : getIndexes())
        {
            postIndex(getDb(), getTb(), i);
        }
        loadData();//actual test here, however it is better to call it here for ordering sake
    }

//    @AfterClass
//    public static void afterTest() throws InterruptedException
//    {
//        deleteData(getDb(), getTb(), getIndexes()); //should delete everything related to this table
//        Thread.sleep(10000); //have to let the deletes finish before shutting down
//    }
    @Override
    public List<Document> getDocumentsFromFS() throws IOException, ParseException
    {
        return Fixtures.getBulkDocuments("./src/test/resources/players.json", getTb());
    }

    @Override
    public List<Document> getDocumentsFromFS(int numToRead) throws IOException, ParseException
    {
        throw new UnsupportedOperationException("Intentionally Unsupported.");
    }

    @Override
    public int getNumDocuments() throws IOException, ParseException
    {
        return getDocumentsFromFS().size();
    }

    /**
     * @return the db
     */
    @Override
    public Database getDb()
    {
        Database db = new Database("players");
        db.description("A database about players.");
        return db;
    }

    /**
     * @return the tb
     */
    @Override
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
    @Override
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

    /**
     * Tests that the POST /{databases}/{table}/query endpoint properly runs a
     * query with a set time.
     */
    @Test
    public void postQueryTest()
    {
        int numQueries = 50;
        Date start = new Date();
        for (int i = 0; i < numQueries; i++)
        {
            logger.debug("Query: " + i);
            given().header("limit", "10000").body("{\"where\":\"NAMELAST = 'Manning'\"}").expect().statusCode(200)
                    //.header("Location", startsWith(RestAssured.basePath + "/"))
                    .body("", notNullValue())
                    .body("id", notNullValue())
                    .when().post(getDb().name() + "/" + getTb().name() + "/queries");
        }
        Date end = new Date();
        long executionTime = end.getTime() - start.getTime();
        double inSeconds = (double) executionTime / 1000d;
        double average = (double) inSeconds / (double) numQueries;
        output.info("Players-Doc: Time to execute (single field) for " + numQueries + " is: " + inSeconds + " seconds");
        output.info("Players-Doc: Averge time for single field is: " + average);
    }

    /**
     * Tests that the POST /{databases}/{table}/query endpoint properly runs a
     * two field query with a set time.
     */
    @Test
    public void postQueryTestTwoField()
    {
        int numQueries = 50;
        Date start = new Date();
        for (int i = 0; i < numQueries; i++)
        {
            logger.debug("Query: " + i);
            given().header("limit", "10000").body("{\"where\":\"NAMELAST = 'Manning' AND NAMEFIRST = 'Peyton'\"}").expect().statusCode(200)
                    //.header("Location", startsWith(RestAssured.basePath + "/"))
                    .body("", notNullValue())
                    .body("id", notNullValue())
                    .when().post(getDb().name() + "/" + getTb().name() + "/queries");
        }
        Date end = new Date();

        long executionTime = end.getTime() - start.getTime();
        double inSeconds = (double) executionTime / 1000d;
        double average = (double) inSeconds / (double) numQueries;
        output.info("Players-Doc: Time to execute (two fields) for " + numQueries + " is: " + inSeconds + " seconds");
        output.info("Players-Doc: Averge time for two fields is: " + average);
    }

    /**
     * Tests that the querying directly with the driver endpoint properly runs a
     * query with a set time (test should always pass; check output for stats.)
     */
    @Test
    @Ignore
    public void directQueryTest()
    {
        int numQueries = 50;
        Fixtures f = Fixtures.getInstance("10.199.12.206,10.199.11.178,10.199.4.114,10.199.24.180,10.199.28.24,10.199.23.159");
        QueryService qs = new QueryService(new QueryRepository(f.getSession()));
        Query q = new Query();
        q.setWhere("NAMELAST = 'Manning'");
        q.setTable(this.getTb().name());

        Date start = new Date();
        for (int i = 0; i < numQueries; i++)
        {
            logger.debug("Query: " + i);
            QueryResponseWrapper response = qs.query(this.getDb().name(), q, 10000, 0);
            for (Document d : response)
            {
                assertNotNull(d);
                String json = d.object();
                assertTrue(json.indexOf("Manning") > -1);
            }
        }
        Date end = new Date();
        long executionTime = end.getTime() - start.getTime();
        double inSeconds = (double) executionTime / 1000d;
        double average = (double) inSeconds / (double) numQueries;
        output.info("Players-Doc-Direct: Time to execute (single field) for " + numQueries + " is: " + inSeconds + " seconds");
        output.info("Players-Doc-Direct: Averge time for single field is: " + average);
    }
}

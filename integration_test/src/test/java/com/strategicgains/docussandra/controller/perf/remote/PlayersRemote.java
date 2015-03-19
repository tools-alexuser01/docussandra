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
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import static org.hamcrest.Matchers.notNullValue;
import org.json.simple.parser.ParseException;
import org.junit.Test;

/**
 * Does a perf test using the players data.
 *
 * @author udeyoje
 */
public class PlayersRemote extends PerfTestParent
{

    public PlayersRemote() throws IOException, InterruptedException, ParseException
    {
        setup();
    }

    protected void setup() throws IOException, InterruptedException, ParseException
    {
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
    protected List<Document> getDocumentsFromFS() throws IOException, ParseException
    {
        return Fixtures.getBulkDocuments("./src/test/resources/players.json", getTb());
    }

    @Override
    protected List<Document> getDocumentsFromFS(int numToRead) throws IOException, ParseException
    {
        throw new UnsupportedOperationException("Intentionally Unsupported.");
    }

    @Override
    protected int getNumDocuments() throws IOException, ParseException
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
        double tpms = (double) numQueries / (double) executionTime;
        double tps = tpms / 1000d;
        output.info("Players: Time to execute (single field) for " + numQueries + " is: " + inSeconds + " seconds");
        output.info("Players: Averge TPS for single field is:" + tps);
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
            given().header("limit", "10000").body("{\"where\":\"NAMELAST = 'Manning'AND NAMEFIRST = 'Peyton'\"}").expect().statusCode(200)
                    //.header("Location", startsWith(RestAssured.basePath + "/"))
                    .body("", notNullValue())
                    .body("id", notNullValue())
                    .when().post(getDb().name() + "/" + getTb().name() + "/queries");
        }
        Date end = new Date();

        long executionTime = end.getTime() - start.getTime();
        double inSeconds = (double) executionTime / 1000d;
        double tpms = (double) numQueries / (double) executionTime;
        double tps = tpms / 1000d;
        output.info("Players: Time to execute (two fields) for " + numQueries + " is: " + inSeconds + " seconds");
        output.info("Players: Averge TPS for two fields is:" + tps);
    }
}

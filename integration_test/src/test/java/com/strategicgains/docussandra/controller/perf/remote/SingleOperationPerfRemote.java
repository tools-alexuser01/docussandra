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
import com.strategicgains.docussandra.controller.perf.remote.parent.PerfTestParent;
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.time.StopWatch;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import org.json.simple.parser.ParseException;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author udeyoje
 */
public class SingleOperationPerfRemote extends PerfTestParent
{

    protected void setup() throws IOException, InterruptedException, ParseException
    {
        logger.info("Setup called!");
        beforeClass();
        deleteData(getDb(), getTb(), getIndexes()); //should delete everything related to this table
        postDB(getDb());
        postTable(getDb(), getTb());
        for (Index i : getIndexes())
        {
            postIndex(getDb(), getTb(), i);
        }
    }

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

    @Test
    @Ignore
    public void testDbCreationTime()
    {
        StopWatch sw = new StopWatch();
        sw.start();
        postDB(getDb());
        sw.stop();
        output.info("Time to create a DB: " + sw.getTime());
    }

    @Test
    public void testInsertAndFetchOneDocTime() throws IOException, ParseException
    {
        Document doc = getDocumentsFromFS().get(0);
        StopWatch sw = new StopWatch();
        Database d = getDb();
        Table t = getTb();
        sw.start();
        String id = postDocument(d, t, doc);
        sw.stop();
        output.info("Time to create a single doc: " + sw.getTime() + " id: " + id);
        sw.reset();
        sw.start();
        String basePath = RestAssured.basePath;
        try
        {
            RestAssured.basePath = "/" + d.name() + "/" + t.name();
            expect().statusCode(200)
                    .body("id", equalTo(id))
                    .get("/" + id);
            sw.stop();
            output.info("Time to fetch a single doc: " + sw.getTime());
        } finally
        {
            RestAssured.basePath = basePath;
        }
    }
}

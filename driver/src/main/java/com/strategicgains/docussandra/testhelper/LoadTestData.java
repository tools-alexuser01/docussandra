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
package com.strategicgains.docussandra.testhelper;

import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.IndexField;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.repoexpress.exception.DuplicateItemException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Just used to load test data.
 *
 * @author udeyoje
 */
public class LoadTestData
{

    private static Logger logger = LoggerFactory.getLogger(LoadTestData.class);

    private Fixtures f;

    public LoadTestData() throws Exception
    {
        f = Fixtures.getInstance(false);//don't mock cassandra here
    }

    public void go() throws Exception
    {
        Database testDb = new Database("players");
        testDb.description("A database about athletic players");
        Table testTable = new Table();
        testTable.name("players");
        testTable.database(testDb.name());
        testTable.description("My Table stores a lot of data about players.");
        try
        {
            f.insertDatabase(testDb);
        } catch (DuplicateItemException e)
        {
            System.out.println("Didn't re-create DB");
            //logger.info("Probile creating DB", e);
        }
        try
        {
            f.insertTable(testTable);
        } catch (DuplicateItemException e)
        {
            System.out.println("Didn't re-create Table");
            //logger.info("Probile creating table", e);
        }
        Index player = new Index("player");
        player.isUnique(false);
        List<IndexField> fields = new ArrayList<>(1);
        fields.add(new IndexField("NAMEFULL"));
        player.fields(fields);
        player.table(testTable);
        try
        {
            f.insertIndex(player);
        } catch (DuplicateItemException e)
        {
            System.out.println("Didn't re-create Index");
            //logger.info("Probile creating index", e);
        }
        Index lastname = new Index("lastname");
        lastname.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add(new IndexField("NAMELAST"));
        lastname.fields(fields);
        lastname.table(testTable);
        try
        {
            f.insertIndex(lastname);
        } catch (DuplicateItemException e)
        {
            System.out.println("Didn't re-create Index");
            //logger.info("Probile creating index", e);
        }
        Index lastAndFirst = new Index("lastandfirst");
        lastAndFirst.isUnique(false);
        fields = new ArrayList<>(2);
        fields.add(new IndexField("NAMELAST"));
        fields.add(new IndexField("NAMEFIRST"));
        lastAndFirst.fields(fields);
        lastAndFirst.table(testTable);
        try
        {
            f.insertIndex(lastAndFirst);
        } catch (DuplicateItemException e)
        {
            System.out.println("Didn't re-create Index");
            //logger.info("Probile creating index", e);
        }
        Index team = new Index("team");
        team.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add(new IndexField("TEAM"));
        team.fields(fields);
        team.table(testTable);
        try
        {
            f.insertIndex(team);
        } catch (DuplicateItemException e)
        {
            System.out.println("Didn't re-create Index");
            //logger.info("Probile creating index", e);
        }

        Index position = new Index("postion");
        position.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add(new IndexField("POSITION"));
        position.fields(fields);
        position.table(testTable);
        try
        {
            f.insertIndex(position);
        } catch (DuplicateItemException e)
        {
            System.out.println("Didn't re-create Index");
            //logger.info("Probile creating index", e);
        }

        Index rookie = new Index("rookieyear");
        rookie.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add(new IndexField("ROOKIEYEAR"));
        rookie.fields(fields);
        rookie.table(testTable);
        try
        {
            f.insertIndex(rookie);
        } catch (DuplicateItemException e)
        {
            System.out.println("Didn't re-create Index");
            //logger.info("Probile creating index", e);
        }

        List<Document> docs = Fixtures.getBulkDocuments("./src/test/resources/players.json", testTable);
        f.insertDocuments(docs);
        System.out.println("Done!");
        System.exit(0);
    }

    public static void main(String[] args) throws Exception
    {
        System.out.println("Loading test data...");
        LoadTestData ltd = new LoadTestData();
        ltd.go();

    }

}

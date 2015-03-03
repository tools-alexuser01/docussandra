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
import com.strategicgains.docussandra.domain.Table;
import java.util.ArrayList;
import java.util.List;

/**
 * Just used to load test data.
 *
 * @author udeyoje
 */
public class LoadTestData
{

    private Fixtures f;

    public LoadTestData()
    {
        f = Fixtures.getInstance();
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
        } catch (Exception e)
        {
            System.out.println("Didn't re-create DB");
        }
        try
        {
            f.insertTable(testTable);
        } catch (Exception e)
        {
            System.out.println("Didn't re-create Table");
        }
        Index player = new Index("player");
        player.isUnique(false);
        List<String> fields = new ArrayList<>(1);
        fields.add("NAMEFULL");
        player.fields(fields);
        try
        {
            f.insertIndex(player);
        } catch (Exception e)
        {
            System.out.println("Didn't re-create Index");
        }
        Index lastname = new Index("lastname");
        lastname.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("NAMELAST");
        lastname.fields(fields);
        try
        {
            f.insertIndex(lastname);
        } catch (Exception e)
        {
            System.out.println("Didn't re-create Index");
        }
        Index team = new Index("team");
        team.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("TEAM");
        team.fields(fields);
        try
        {
            f.insertIndex(team);
        } catch (Exception e)
        {
            System.out.println("Didn't re-create Index");
        }

        Index position = new Index("postion");
        position.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("POSITION");
        position.fields(fields);
        try
        {
            f.insertIndex(position);
        } catch (Exception e)
        {
            System.out.println("Didn't re-create Index");
        }

        Index rookie = new Index("rookieyear");
        rookie.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("ROOKIEYEAR");
        rookie.fields(fields);
        try
        {
            f.insertIndex(rookie);
        } catch (Exception e)
        {
            System.out.println("Didn't re-create Index");
        }

        List<Document> docs = Fixtures.getBulkDocuments("./src/test/resources/players.json");
        f.insertDocuments(docs);

    }

    public static void main(String[] args) throws Exception
    {
        System.out.println("Loading test data...");
        LoadTestData ltd = new LoadTestData();
        ltd.go();

    }

}

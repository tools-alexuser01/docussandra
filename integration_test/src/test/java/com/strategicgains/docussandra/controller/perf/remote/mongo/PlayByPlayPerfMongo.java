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
package com.strategicgains.docussandra.controller.perf.remote.mongo;

import com.strategicgains.docussandra.controller.perf.remote.*;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import org.json.simple.parser.ParseException;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Does a perf test using the play by play data. PBB.json must be in your home
 * directory.
 *
 * @author udeyoje
 */
public class PlayByPlayPerfMongo extends PlayByPlayRemote
{

    private static MongoClientURI uri;

    public PlayByPlayPerfMongo() throws IOException, InterruptedException, ParseException
    {
        super();
    }

    @Override
    protected void setup() throws IOException, InterruptedException, ParseException
    {
        logger.info("Setup called!");
        beforeClass();
        uri = new MongoClientURI("mongodb://10.199.11.182:27017/?j=true");
        loadData();//actual test here, however it is better to call it here for ordering sake
    }

    @Override
    public void loadData()
    {
        try
        {
            MongoLoader.loadMongoData(uri, NUM_WORKERS, this.getDb(), this.getNumDocuments(), this);
        } catch (IOException e)
        {
            logger.error("Couldn't load documents.");
        }
    }

    @Test
    @Override
    public void postQueryTest()
    {
        try
        {
            int numQueries = 50;
            Date start = new Date();
            MongoClient mongoClient = new MongoClient(uri);
            //mongoClient.setWriteConcern(WriteConcern.MAJORITY);
            DB db = mongoClient.getDB(this.getDb().name());
            final DBCollection coll = db.getCollection(this.getDb().name());
            for (int i = 0; i < numQueries; i++)
            {
                logger.debug("Query: " + i);
                ArrayList<String> res = new ArrayList<>();
                DBCursor curser = coll.find(new BasicDBObject("dwn", 4));
                int count = 0;
                while (curser.hasNext() && count++ < 10000)
                {
                    DBObject o = curser.next();
                    assertNotNull(o);
                    assertTrue(o.toString().contains("4"));
                    res.add(o.toString());
                }
                assertFalse(res.isEmpty());
            }
            Date end = new Date();
            long executionTime = end.getTime() - start.getTime();
            double inSeconds = (double) executionTime / 1000d;
            double average = (double) inSeconds / (double) numQueries;
            output.info("PBP-Mongo: Time to execute (single field) for " + numQueries + " is: " + inSeconds + " seconds");
            output.info("PBP-Mongo: Averge time for single field is: " + average);
        } catch (UnknownHostException e)
        {
            logger.error("Couldn't run test.", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    @Override
    public void postQueryTestTwoField()
    {
        try
        {
            int numQueries = 50;
            Date start = new Date();
            MongoClient mongoClient = new MongoClient(uri);
            //mongoClient.setWriteConcern(WriteConcern.MAJORITY);
            DB db = mongoClient.getDB(this.getDb().name());
            final DBCollection coll = db.getCollection(this.getDb().name());
            for (int i = 0; i < numQueries; i++)
            {
                logger.debug("Query: " + i);
                ArrayList<String> res = new ArrayList<>();
                BasicDBObject query = new BasicDBObject();
                query.append("dwn", 4);
                query.append("ytg", 1);
                DBCursor curser = coll.find(query);
                int count = 0;
                while (curser.hasNext() && count++ < 10000)
                {
                    DBObject o = curser.next();
                    assertNotNull(o);
                    assertTrue(o.toString().contains("4"));
                    assertTrue(o.toString().contains("1"));
                    res.add(o.toString());
                }
                assertFalse(res.isEmpty());
            }
            Date end = new Date();

            long executionTime = end.getTime() - start.getTime();
            double inSeconds = (double) executionTime / 1000d;
            double average = (double) inSeconds / (double) numQueries;
            output.info("PBP-Mongo: Time to execute (two fields) for " + numQueries + " is: " + inSeconds + " seconds");
            output.info("PBP-Mongo: Averge time for two fields is: " + average);
        } catch (UnknownHostException e)
        {
            logger.error("Couldn't run test.", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    @Override
    public void postQueryTestThreeField()
    {
        try
        {
            int numQueries = 50;
            Date start = new Date();
            MongoClient mongoClient = new MongoClient(uri);
            //mongoClient.setWriteConcern(WriteConcern.MAJORITY);
            DB db = mongoClient.getDB(this.getDb().name());
            final DBCollection coll = db.getCollection(this.getDb().name());
            for (int i = 0; i < numQueries; i++)
            {
                logger.debug("Query: " + i);
                ArrayList<String> res = new ArrayList<>();
                BasicDBObject query = new BasicDBObject();
                query.append("dwn", 4);
                query.append("ytg", 1);
                query.append("pts", 6);
                DBCursor curser = coll.find(query);
                int count = 0;
                while (curser.hasNext() && count++ < 10000)
                {
                    DBObject o = curser.next();
                    assertNotNull(o);
                    assertTrue(o.toString().contains("4"));
                    assertTrue(o.toString().contains("1"));
                    assertTrue(o.toString().contains("6"));
                    res.add(o.toString());
                }
                assertFalse(res.isEmpty());
            }
            Date end = new Date();

            long executionTime = end.getTime() - start.getTime();
            double inSeconds = (double) executionTime / 1000d;
            double average = (double) inSeconds / (double) numQueries;
            output.info("PBP-Mongo: Time to execute (three fields) for " + numQueries + " is: " + inSeconds + " seconds");
            output.info("PBP-Mongo: Averge time for three fields is: " + average);
        } catch (UnknownHostException e)
        {
            logger.error("Couldn't run test.", e);
            throw new RuntimeException(e);
        }
    }

}

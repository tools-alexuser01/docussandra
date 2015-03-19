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
import static com.jayway.restassured.RestAssured.given;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;
import com.strategicgains.docussandra.domain.Document;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import static org.hamcrest.CoreMatchers.notNullValue;
import org.json.simple.parser.ParseException;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Does a perf test using the play by play data. PBB.json must be in your home
 * directory.
 *
 * @author udeyoje
 */
public class PlayByPlayPerfMongo extends PlayByPlayRemote
{

    public PlayByPlayPerfMongo() throws IOException, InterruptedException, ParseException
    {
        super();
    }

    @Override
    protected void setup() throws IOException, InterruptedException, ParseException
    {
        beforeClass();
        loadData();//actual test here, however it is better to call it here for ordering sake
    }

    @Override
    public void loadData()
    {
        try
        {
            try
            {
                MongoClientURI uri = new MongoClientURI("mongodb://localhost/?uri.journal=true");
                MongoClient mongoClient = new MongoClient(uri);
                mongoClient.setWriteConcern(WriteConcern.MAJORITY);
                DB db = mongoClient.getDB(this.getDb().name());
                final DBCollection coll = db.getCollection(this.getDb().name());
                ArrayList<Thread> workers = new ArrayList<>(NUM_WORKERS + 1);
                int numDocs = super.getNumDocuments();
                int docsPerWorker = numDocs / NUM_WORKERS;
                try
                {
                    List<Document> docs = getDocumentsFromFS();
                    ArrayList<List<Document>> documentQueues = new ArrayList<>(NUM_WORKERS + 1);
                    int numDocsAssigned = 0;
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
                    for (final List<Document> queue : documentQueues)
                    {
                        workers.add(new Thread()
                        {
                            @Override
                            public void run()
                            {
                                for (Document d : queue)
                                {
                                    DBObject o = (DBObject) JSON.parse(d.object());
                                    coll.save(o);
                                }
                                logger.info("Thread " + Thread.currentThread().getName() + " is done.");
                            }
                        });
                    }
                } catch (UnsupportedOperationException e)//we can't read everything in at once
                {
                    //all we need to do in this block is find a way to set "workers"
                    for (int i = 0; i < NUM_WORKERS; i++)
                    {
                        workers.add(new Thread()
                        {
                            private final int chunk = (int) (Math.random() * 100) + 150;//pick a random chunk so we are not going back to the FS all at the same time and potentially causing a bottle neck

                            @Override
                            public void run()
                            {
                                int counter = 0;
                                try
                                {
                                    List<Document> docs = getDocumentsFromFS(chunk);//grab a handful of documents
                                    while (docs.size() > 0)
                                    {
                                        for (Document d : docs)//process the documents we grabbed
                                        {
                                            DBObject o = (DBObject) JSON.parse(d.object());
                                            coll.save(o);
                                            counter++;
                                        }
                                        docs = getDocumentsFromFS(chunk);//grab another handful of documents
                                    }
                                    logger.info("Thread " + Thread.currentThread().getName() + " is done. It processed " + counter + " documents.");
                                } catch (IOException | ParseException e)
                                {
                                    logger.error("Couldn't read from document", e);
                                }
                            }
                        });
                    }
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
                        try
                        {
                            Thread.sleep(10000);
                        } catch (InterruptedException e)
                        {
                        }
                    }
                }
                long end = new Date().getTime();
                long miliseconds = end - start;
                double seconds = (double) miliseconds / 1000d;
                output.info("Done loading data using: " + NUM_WORKERS + " and URL: " + BASE_URI + ". Took: " + seconds + " seconds");
                double tpms = (double) numDocs / (double) miliseconds;
                double tps = tpms * 1000;
                double transactionTime = (double) tft.get() / (double) numDocs;
                output.info("Average Transactions Per Second: " + tps);
                output.info("Average Transactions Time (in miliseconds): " + transactionTime);

            } catch (UnknownHostException e)
            {
                logger.error("Couldn't connect to Mongo Server", e);
            }
        } catch (IOException | ParseException e)
        {
            logger.error("Couldn't read data.", e);
        }
    }

    @Test
    @Override
    public void postQueryTest()
    {
        int numQueries = 1000;
        Date start = new Date();
        for (int i = 0; i < numQueries; i++)
        {
            logger.debug("Query: " + i);
//            given().header("limit", "10000").body("{\"where\":\"dwn = '4'\"}").expect().statusCode(200)
//                    //.header("Location", startsWith(RestAssured.basePath + "/"))
//                    .body("", notNullValue())
//                    .body("id", notNullValue())
//                    .when().post("");
        }
        Date end = new Date();
        long executionTime = end.getTime() - start.getTime();
        double inSeconds = (double) executionTime / 1000d;
        double tpms = (double) numQueries / (double) executionTime;
        double tps = tpms / 1000d;
        output.info("PBP: Time to execute (single field) for " + numQueries + " is: " + inSeconds + " seconds");
        output.info("PBP: Averge TPS for single field is:" + tps);
    }

    @Test
    @Override
    public void postQueryTestTwoField()
    {
        int numQueries = 1000;
        Date start = new Date();
        for (int i = 0; i < numQueries; i++)
        {
            logger.debug("Query: " + i);
//            given().header("limit", "10000").body("{\"where\":\"dwn = '4' AND ytg = '1'\"}").expect().statusCode(200)
//                    //.header("Location", startsWith(RestAssured.basePath + "/"))
//                    .body("", notNullValue())
//                    .body("id", notNullValue())
//                    .when().post("");
        }
        Date end = new Date();

        long executionTime = end.getTime() - start.getTime();
        double inSeconds = (double) executionTime / 1000d;
        double tpms = (double) numQueries / (double) executionTime;
        double tps = tpms / 1000d;
        output.info("PBP: Time to execute (two fields) for " + numQueries + " is: " + inSeconds + " seconds");
        output.info("PBP: Averge TPS for two fields is:" + tps);
    }

    @Test
    @Override
    public void postQueryTestThreeField()
    {
        int numQueries = 1000;
        Date start = new Date();
        for (int i = 0; i < numQueries; i++)
        {
            logger.debug("Query: " + i);
//            given().header("limit", "10000").body("{\"where\":\"dwn = '4' AND ytg = '1' AND pts = '6'\"}").expect().statusCode(200)
//                    //.header("Location", startsWith(RestAssured.basePath + "/"))
//                    .body("", notNullValue())
//                    .body("id", notNullValue())
//                    .when().post("");
        }
        Date end = new Date();

        long executionTime = end.getTime() - start.getTime();
        double inSeconds = (double) executionTime / 1000d;
        double tpms = (double) numQueries / (double) executionTime;
        double tps = tpms / 1000d;
        output.info("PBP: Time to execute (three fields) for " + numQueries + " is: " + inSeconds + " seconds");
        output.info("PBP: Averge TPS for three fields is:" + tps);
    }

}

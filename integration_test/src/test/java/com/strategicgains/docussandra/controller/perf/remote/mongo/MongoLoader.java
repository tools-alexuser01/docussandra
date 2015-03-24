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

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;
import com.strategicgains.docussandra.controller.perf.remote.parent.PerfTestParent;
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Document;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ugly mongo loader.
 *
 * @author udeyoje
 */
public class MongoLoader
{

    protected static final Logger logger = LoggerFactory.getLogger(MongoLoader.class);
    protected static final Logger output = LoggerFactory.getLogger("output");

    public static void loadMongoData(MongoClientURI uri, final int NUM_WORKERS, Database database, final int numDocs, final PerfTestParent clazz)
    {
        logger.info("------------Loading Data into: " + database.name() + " with MONGO!------------");
        try
        {
            try
            {
                MongoClient mongoClient = new MongoClient(uri);
                mongoClient.setWriteConcern(WriteConcern.MAJORITY);
                DB db = mongoClient.getDB(database.name());
                final DBCollection coll = db.getCollection(database.name());
                ArrayList<Thread> workers = new ArrayList<>(NUM_WORKERS + 1);
                int docsPerWorker = numDocs / NUM_WORKERS;
                try
                {
                    List<Document> docs = clazz.getDocumentsFromFS();
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
                                ThreadLocal<Integer> counter = new ThreadLocal<>();
                                counter.set(new Integer(0));
                                try
                                {
                                    List<Document> docs = clazz.getDocumentsFromFS(chunk);//grab a handful of documents
                                    while (docs.size() > 0)
                                    {
                                        for (Document d : docs)//process the documents we grabbed
                                        {
                                            DBObject o = (DBObject) JSON.parse(d.object());
                                            coll.save(o);
                                            counter.set(counter.get() + 1);
                                        }
                                        docs = clazz.getDocumentsFromFS(chunk);//grab another handful of documents
                                    }
                                    logger.info("Thread " + Thread.currentThread().getName() + " is done. It processed " + counter.get() + " documents.");
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
                output.info("Done loading data using: " + NUM_WORKERS + ". Took: " + seconds + " seconds");
                double tpms = (double) numDocs / (double) miliseconds;
                double tps = tpms * 1000;
                double transactionTime = (double) miliseconds / (double) numDocs;
                output.info(database.name() + " Mongo Average Transactions Per Second: " + tps);
                output.info(database.name() + " Mongo Average Transactions Time (in miliseconds): " + transactionTime);

            } catch (UnknownHostException e)
            {
                logger.error("Couldn't connect to Mongo Server", e);
            }
        } catch (IOException | ParseException e)
        {
            logger.error("Couldn't read data.", e);
        }
    }
}

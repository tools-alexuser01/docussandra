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
package com.strategicgains.docussandra.controller.perf.remote.parent;

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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.time.StopWatch;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import org.json.simple.parser.ParseException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs a perf test using the overridden data. 1. Creates a DB. 2. Creates a
 * setTable. 3. Creates all the indexes. 4. Inserts all the documents. 5. Cleans
 * up anything it did. Treat this as a singleton, even though it is not.
 *
 * @author udeyoje
 */
public abstract class PerfTestParent
{

    protected static final Logger logger = LoggerFactory.getLogger(PerfTestParent.class);
    protected static final Logger output = LoggerFactory.getLogger("output");
    protected static final String BASE_URI = "https://docussandra.stg-prsn.com";//; "http://localhost";//
    //protected static final String BASE_URI = "http://localhost";
    //protected static final String BASE_URI = "http://localhost:8081";
    //protected static final String BASE_URI = "http://10.199.30.15:8080";
    //protected static final int PORT = 19080;
    protected static final int NUM_WORKERS = 50; //NOTE: one more worker may be added to pick up any remainder
    protected static AtomicInteger errorCount = new AtomicInteger(0);
    //protected static AtomicLong tft = new AtomicLong(0);

    @BeforeClass
    public static void beforeClass() throws IOException
    {
        //RestExpressManager.getManager().ensureRestExpressRunning();
        RestAssured.baseURI = BASE_URI;
        //RestAssured.port = PORT;
        RestAssured.basePath = "/";
        RestAssured.useRelaxedHTTPSValidation();
    }

    @AfterClass
    public static void afterClass() throws InterruptedException
    {
        Thread.sleep(10000); //have to let the deletes finish before shutting down
    }

    protected static void postDB(Database database)
    {
        logger.debug("Creating test DB");
        String dbStr = "{" + "\"description\" : \"" + database.description() + "\"," + "\"name\" : \"" + database.name() + "\"}";
        //act
        given().body(dbStr).expect().when().post(database.name());
        //check
        expect().statusCode(200).body("name", equalTo(database.name())).body("description", equalTo(database.description())).body("createdAt", notNullValue()).body("updatedAt", notNullValue()).when().get("/" + database.name());
    }

    protected static void deleteData(Database database, Table table, List<Index> index)
    {
        logger.debug("Deleteing test DB");
        //act
        given().when().delete(database.name());
        given().when().delete(database.name() + "/" + table.name());
        for (Index i : index)
        {
            given().when().delete(database.name() + "/" + table.name() + "/indexes/" + i.getName());
        }
    }

    protected static String postDocument(Database database, Table table, Document d)
    {
        //setup
        //set rookie year to be empty so we don't cause more problems than we need
        String object = d.object().replaceAll("\\Q\"ROOKIEYEAR\":\"\"\\E", "");
        d.object(object);
        //act
        //long start = new Date().getTime();
        Response response = given().body(d.object()).expect().when().post(database.name() + "/" + table.name() + "/").andReturn();
        //long end = new Date().getTime();
        //long tftt = end - start;
        //tft.addAndGet(tftt);
        int code = response.getStatusCode();
        if (code != 201)
        {
            errorCount.addAndGet(1);
            logger.info("Error publishing document: " + response.getBody().prettyPrint());
            logger.info("The document was: " + d.toString());
            logger.info("This is the: " + errorCount.toString() + " error.");
        }
        return response.getBody().jsonPath().get("id");
    }

    //@Test
    public void loadData() throws IOException, ParseException, InterruptedException
    {
        logger.info("------------Loading Data into: " + this.getDb().name() + " with Docussandra!------------");
        ArrayList<Thread> workers = new ArrayList<>(NUM_WORKERS + 1);
        int numDocs = getNumDocuments();
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
                            //logger.debug("Processing document: " + d.toString());
                            postDocument(getDb(), getTb(), d);
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
                            List<Document> docs = getDocumentsFromFS(chunk);//grab a handful of documents
                            while (docs.size() > 0)
                            {
                                for (Document d : docs)//process the documents we grabbed
                                {
                                    //logger.debug("Processing document: " + d.toString());
                                    postDocument(getDb(), getTb(), d);//post them up
                                    counter.set(counter.get() + 1);
                                }
                                docs = getDocumentsFromFS(chunk);//grab another handful of documents
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

        //long start = new Date().getTime();
        StopWatch sw = new StopWatch();
        sw.start();
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
                sw.stop();
            } else
            {
                logger.info("We still have workers running...");
                Thread.sleep(5000);
            }
        }

        long miliseconds = sw.getTime();
        double seconds = (double) miliseconds / 1000d;
        output.info("Doc: Done loading data using: " + NUM_WORKERS + " and URL: " + BASE_URI + ". Took: " + seconds + " seconds");
        double tpms = (double) numDocs / (double) miliseconds;
        double tps = tpms * 1000;
        double transactionTime = (double) miliseconds / (double) numDocs;
        output.info(this.getDb().name() + " Doc: Average Transactions Per Second: " + tps);
        output.info(this.getDb().name() + " Doc: Average Transactions Time (in miliseconds): " + transactionTime);
        Thread.sleep(100000);//sleep a bit to let the DB digest that before trying anything else
    }

    protected static void postTable(Database database, Table table)
    {
        logger.debug("Creating test table");
        String tableStr = "{" + "\"description\" : \"" + table.description() + "\"," + "\"name\" : \"" + table.name() + "\"}";
        //act
        given().body(tableStr).expect().when().post(database.name() + "/" + table.name());
        //check
        expect().statusCode(200).body("name", equalTo(table.name())).body("description", equalTo(table.description())).body("createdAt", notNullValue()).body("updatedAt", notNullValue()).when().get(database.name() + "/" + table.name());
    }

    protected static void postIndex(Database database, Table table, Index index)
    {
        logger.info("POSTing index: " + index.toString());
        String tableStr = Fixtures.generateIndexCreationStringWithFields(index);
        //act
        given().body(tableStr.toString()).when().post(database.name() + "/" + table.name() + "/indexes/" + index.getName());
        //check
        Response r = expect().statusCode(200).body("name", equalTo(index.getName())).body("fields", notNullValue()).body("createdAt", notNullValue()).body("updatedAt", notNullValue()).get(database.name() + "/" + table.name() + "/indexes/" + index.getName()).andReturn();
        logger.debug("Index created: \r\n" + r.getBody().prettyPrint());
    }

    /**
     * Gets the database to use for this test.
     *
     * @return the db
     */
    public abstract Database getDb();

    /**
     * Gets the setTable to use for this test.
     *
     * @return the tb
     */
    public abstract Table getTb();

    /**
     * Gets the indexes to use for this test.
     *
     * @return the indexes
     */
    public abstract List<Index> getIndexes();

    /**
     * Gets the documents to use for this test.
     *
     * At least one of the getDocumentsFromFS methods must be implemented. Throw
     * an UnsupportedOperationException if you are not implementing. This method
     * is tried first. Used for reading all the documents into memory at once.
     *
     * @return
     * @throws IOException
     * @throws ParseException
     */
    public abstract List<Document> getDocumentsFromFS() throws IOException, ParseException;

    /**
     * Gets the documents to use for this test.
     *
     * At least one of the getDocumentsFromFS methods must be implemented. Throw
     * an UnsupportedOperationException if you are not implementing. This method
     * is tried second. Used for reading a limited number of documents at once.
     *
     * @param numToRead Number of documents to read at once.
     * @return
     * @throws IOException
     * @throws ParseException
     */
    public abstract List<Document> getDocumentsFromFS(int numToRead) throws IOException, ParseException;

    /**
     * Gets the number of documents available for reading.
     *
     * @return
     * @throws IOException
     */
    public abstract int getNumDocuments() throws IOException, ParseException;
}

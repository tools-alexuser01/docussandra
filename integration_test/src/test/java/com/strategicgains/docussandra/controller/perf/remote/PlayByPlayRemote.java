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
import com.strategicgains.docussandra.domain.IndexField;
import com.strategicgains.docussandra.domain.Table;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import static org.hamcrest.CoreMatchers.notNullValue;
import org.json.simple.parser.ParseException;
import org.junit.Test;

/**
 * Does a perf test using the play by play data. PBB.json must be in your home
 * directory.
 *
 * @author udeyoje
 */
public class PlayByPlayRemote extends PerfTestParent
{

    private static AtomicInteger position = new AtomicInteger(0);

    public static final String path = "PBP.json";

    private static boolean setupRun = false;

    public PlayByPlayRemote() throws IOException, InterruptedException, ParseException
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
        deleteData(getDb(), getTb(), getIndexes()); //should delete everything related to this
        postDB(getDb());
        postTable(getDb(), getTb());
        for (Index i : getIndexes())
        {
            postIndex(getDb(), getTb(), i);
        }
        loadData();//actual test here, however it is better to call it here for ordering sake
    }

    @Override
    public List<Document> getDocumentsFromFS() throws IOException, ParseException
    {
        throw new UnsupportedOperationException("Intentionally Unsupported.");
    }

    @Override
    public List<Document> getDocumentsFromFS(int numToRead) throws IOException, ParseException
    {
        File file = new File(System.getProperty("user.home"), path);
        logger.info("Data path: " + file.getAbsolutePath());
        List<Document> toReturn = new ArrayList<>(numToRead);
        int counter = 0;
        synchronized (this)
        {
            numToRead = numToRead + position.intValue();
            try (BufferedReader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8))
            {
                String line = null;
                while ((line = reader.readLine()) != null)
                {
                    if (counter < position.intValue())
                    {
                        //read up to where we need to go. there has to be a better way to do this, however, not worth it right now
                        counter++;
                    } else if (counter >= position.intValue() && counter < numToRead)
                    {
                        //we have a section we care about
                        //so we add the document
                        Document doc = new Document();
                        doc.table(getTb());
                        doc.setUuid(new UUID(Long.MAX_VALUE - position.intValue(), 1));//give it a UUID that we will reconize
                        doc.object(line);
                        toReturn.add(doc);
                        position.addAndGet(1);//jump the postition
                        counter++;//and the counter
                    } else
                    {
                        logger.info("Exausted all documents in the PBP.json file for this chunk position: " + position.get());
                        break;//we are done
                    }

                }
            }
        }
        return toReturn;
    }

    @Override
    public int getNumDocuments() throws IOException
    {
        File file = new File(System.getProperty("user.home"), path);
        logger.info("Data path: " + file.getAbsolutePath());
        InputStream is = new BufferedInputStream(new FileInputStream(file));
        int count = 0;
        try
        {
            byte[] c = new byte[1024];

            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1)
            {
                empty = false;
                for (int i = 0; i < readChars; ++i)
                {
                    if (c[i] == '\n')
                    {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally
        {
            is.close();
        }
    }

    /**
     * @return the db
     */
    @Override
    public Database getDb()
    {
        Database db = new Database("playbyplay");
        db.description("A database about play by play statistics.");
        return db;
    }

    /**
     * @return the tb
     */
    @Override
    public Table getTb()
    {
        Table tb = new Table();
        tb.name("play_table");
        tb.description("A table about play by play statistics.");
        return tb;
    }

    /**
     * @return the indexes
     */
    @Override
    public List<Index> getIndexes()
    {
        ArrayList<Index> indexes = new ArrayList<>(6);
        Index qtr = new Index("qtr");
        qtr.isUnique(false);
        List<IndexField> fields = new ArrayList<>(1);
        fields.add(new IndexField("qtr"));
        qtr.setFields(fields);
        qtr.setTable(getTb());
        qtr.isUnique(false);

        Index off = new Index("off");
        off.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add(new IndexField("off"));
        off.setFields(fields);
        off.setFields(fields);
        off.setTable(getTb());

        Index def = new Index("def");
        def.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add(new IndexField("def"));
        def.setFields(fields);
        def.setFields(fields);
        def.setTable(getTb());

        Index dwn = new Index("dwn");
        dwn.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add(new IndexField("dwn"));
        dwn.setFields(fields);
        dwn.setFields(fields);
        dwn.setTable(getTb());

        Index dwnAndYtg = new Index("dwnandytg");
        dwnAndYtg.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add(new IndexField("dwn"));
        fields.add(new IndexField("ytg"));
        dwnAndYtg.setFields(fields);
        dwnAndYtg.setFields(fields);
        dwnAndYtg.setTable(getTb());

        Index dwnAndYtgAndPts = new Index("dwnandytgandpts");
        dwnAndYtgAndPts.isUnique(false);
        fields = new ArrayList<>(3);
        fields.add(new IndexField("dwn"));
        fields.add(new IndexField("ytg"));
        fields.add(new IndexField("pts"));
        dwnAndYtgAndPts.setFields(fields);
        dwnAndYtgAndPts.setFields(fields);
        dwnAndYtgAndPts.setTable(getTb());

        Index offAndPts = new Index("offandpts");
        offAndPts.isUnique(false);
        fields = new ArrayList<>(2);
        fields.add(new IndexField("off"));
        fields.add(new IndexField("pts"));
        offAndPts.setFields(fields);
        offAndPts.setFields(fields);
        offAndPts.setTable(getTb());

        indexes.add(qtr);
        indexes.add(off);
        indexes.add(def);
        indexes.add(dwn);
        indexes.add(dwnAndYtg);
        indexes.add(dwnAndYtgAndPts);
        indexes.add(offAndPts);

        return indexes;
    }

    /**
     * Tests that the POST /{databases}/{setTable}/query endpoint properly runs a
 query with a set time.
     */
    @Test
    public void postQueryTest()
    {
        int numQueries = 50;
        Date start = new Date();
        for (int i = 0; i < numQueries; i++)
        {
            logger.debug("Query: " + i);
            given().header("limit", "10000").body("{\"where\":\"dwn = '4'\"}").expect()
                    //.header("Location", startsWith(RestAssured.basePath + "/"))
                    .body("", notNullValue())
                    .body("id", notNullValue())
                    .when().log().ifError().post(getDb().name() + "/" + getTb().name() + "/queries");
        }
        Date end = new Date();
        long executionTime = end.getTime() - start.getTime();
        double inSeconds = (double) executionTime / 1000d;
        double average = (double) inSeconds / (double) numQueries;
        output.info("PBP-doc: Time to execute (single field) for " + numQueries + " is: " + inSeconds + " seconds");
        output.info("PBP-doc: Averge time for single field is: " + average);
    }

    /**
     * Tests that the POST /{databases}/{setTable}/query endpoint properly runs a
 two field query with a set time.
     */
    @Test
    public void postQueryTestTwoField()
    {
        int numQueries = 50;
        Date start = new Date();
        for (int i = 0; i < numQueries; i++)
        {
            logger.debug("Query: " + i);
            given().header("limit", "10000").body("{\"where\":\"dwn = '4' AND ytg = '1'\"}").expect().statusCode(200)
                    //.header("Location", startsWith(RestAssured.basePath + "/"))
                    .body("", notNullValue())
                    .body("id", notNullValue())
                    .when().log().ifError().post(getDb().name() + "/" + getTb().name() + "/queries");
        }
        Date end = new Date();

        long executionTime = end.getTime() - start.getTime();
        double inSeconds = (double) executionTime / 1000d;
        double average = (double) inSeconds / (double) numQueries;
        output.info("PBP-Doc: Time to execute (two fields) for " + numQueries + " is: " + inSeconds + " seconds");
        output.info("PBP-Doc: Averge time for two fields is: " + average);
    }

    /**
     * Tests that the POST /{databases}/{setTable}/query endpoint properly runs a
 two field query with a set time.
     */
    @Test
    public void postQueryTestThreeField()
    {
        int numQueries = 50;
        Date start = new Date();
        for (int i = 0; i < numQueries; i++)
        {
            logger.debug("Query: " + i);
            given().header("limit", "10000").body("{\"where\":\"dwn = '4' AND ytg = '1' AND pts = '6'\"}").expect().statusCode(200)
                    //.header("Location", startsWith(RestAssured.basePath + "/"))
                    .body("", notNullValue())
                    .body("id", notNullValue())
                    .when().post(getDb().name() + "/" + getTb().name() + "/queries");
        }
        Date end = new Date();

        long executionTime = end.getTime() - start.getTime();
        double inSeconds = (double) executionTime / 1000d;
        double average = (double) inSeconds / (double) numQueries;
        output.info("PBP-Doc: Time to execute (three fields) for " + numQueries + " is: " + inSeconds + " seconds");
        output.info("PBP-Doc: Averge time for three fields is: " + average);
    }

}

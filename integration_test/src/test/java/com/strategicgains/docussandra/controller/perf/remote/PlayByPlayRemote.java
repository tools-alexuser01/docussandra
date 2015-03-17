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

import com.strategicgains.docussandra.controller.perf.remote.parent.PerfTestParent;
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.json.simple.parser.ParseException;

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

    @Override
    protected List<Document> getDocumentsFromFS() throws IOException, ParseException
    {
        throw new UnsupportedOperationException("Intentionally Unsupported.");
    }

    @Override
    protected List<Document> getDocumentsFromFS(int numToRead) throws IOException, ParseException
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
                        logger.info("Exausted all documents in the PBP.json file for this chunk: " + position.get());
                        break;//we are done
                    }

                }
            }
        }
        return toReturn;
    }

    @Override
    protected int getNumDocuments() throws IOException
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
        tb.name("players_table");
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
        List<String> fields = new ArrayList<>(1);
        fields.add("qtr");
        qtr.fields(fields);
        qtr.table(getTb());
        qtr.isUnique(false);

        Index off = new Index("off");
        off.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("off");
        off.fields(fields);
        off.fields(fields);
        off.table(getTb());

        Index def = new Index("def");
        def.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("def");
        def.fields(fields);
        def.fields(fields);
        def.table(getTb());

        Index dwn = new Index("dwn");
        dwn.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("dwn");
        dwn.fields(fields);
        dwn.fields(fields);
        dwn.table(getTb());

        Index dwnAndYtg = new Index("dwnandytg");
        dwnAndYtg.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("dwn");
        fields.add("ytg");
        dwnAndYtg.fields(fields);
        dwnAndYtg.fields(fields);
        dwnAndYtg.table(getTb());

        Index dwnAndYtgAndPts = new Index("dwnandytgandpts");
        dwnAndYtgAndPts.isUnique(false);
        fields = new ArrayList<>(3);
        fields.add("dwn");
        fields.add("ytg");
        fields.add("pts");
        dwnAndYtgAndPts.fields(fields);
        dwnAndYtgAndPts.fields(fields);
        dwnAndYtgAndPts.table(getTb());

        Index offAndPts = new Index("offandpts");
        offAndPts.isUnique(false);
        fields = new ArrayList<>(2);
        fields.add("off");
        fields.add("pts");
        offAndPts.fields(fields);
        offAndPts.fields(fields);
        offAndPts.table(getTb());

        indexes.add(qtr);
        indexes.add(off);
        indexes.add(def);
        indexes.add(dwn);
        indexes.add(dwnAndYtg);
        indexes.add(dwnAndYtgAndPts);
        indexes.add(offAndPts);

        return indexes;
    }

}

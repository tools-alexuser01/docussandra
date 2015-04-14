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
package com.strategicgains.docussandra.handler;

import com.strategicgains.docussandra.Utils;
import com.strategicgains.docussandra.cache.CacheFactory;
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.IndexCreationStatus;
import com.strategicgains.docussandra.persistence.DocumentRepository;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.IndexStatusRepository;
import com.strategicgains.docussandra.persistence.IndexStatusRepositoryTest;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.io.IOException;
import java.util.UUID;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author udeyoje
 */
public class BackgroundIndexHandlerTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexStatusRepositoryTest.class);
    private Fixtures f;

    private IndexRepository indexRepo;
    private IndexStatusRepository statusRepo;
    private DocumentRepository docRepo;
    
    public BackgroundIndexHandlerTest() throws Exception
    {
        f = Fixtures.getInstance(true);
    }

    @BeforeClass
    public static void setUpClass()
    {
    }

    @AfterClass
    public static void tearDownClass()
    {
    }

    @Before
    public void setUp() throws IOException, InterruptedException, ParseException
    {
        Utils.initDatabase("/docussandra.cql", f.getSession());//hard clear of the test tables
        Thread.sleep(5000);
        CacheFactory.clearAllCaches();
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        f.insertTable(Fixtures.createTestTable());
        f.insertDocuments(Fixtures.getBulkDocuments());
        indexRepo = new IndexRepository(f.getSession());
        statusRepo = new IndexStatusRepository(f.getSession());
    }

    @After
    public void tearDown()
    {
    }

    /**
     * Test of handles method, of class BackgroundIndexHandler.
     */
    @Test
    public void testHandles()
    {
        System.out.println("handles");
        Class eventClass = UUID.class;
        BackgroundIndexHandler instance = new BackgroundIndexHandler(indexRepo, statusRepo, docRepo);
        boolean result = instance.handles(eventClass);
        assertEquals(true, result);
        Object o = new Object();
        result = instance.handles(o.getClass());
        assertEquals(false, result);
    }

    /**
     * Test of handle method, of class BackgroundIndexHandler.
     */
    @Test
    public void testHandle() throws Exception
    {
        System.out.println("handle");
        //datasetup
        f.insertIndex(Fixtures.createTestIndexOneField());
        IndexCreationStatus entity = Fixtures.createTestIndexCreationStatus();
        statusRepo.createEntity(entity);
        Object event = entity.getUuid();
        //end data setup
        BackgroundIndexHandler instance = new BackgroundIndexHandler(indexRepo, statusRepo, docRepo);
        
        instance.handle(event);

    }

}

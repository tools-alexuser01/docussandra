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
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.event.IndexCreatedEvent;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.persistence.DocumentRepository;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.IndexStatusRepository;
import com.strategicgains.docussandra.persistence.IndexStatusRepositoryTest;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.io.IOException;
import java.util.Date;
import java.util.List;
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
public class IndexCreatedHandlerTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexStatusRepositoryTest.class);
    private Fixtures f;

    private IndexRepository indexRepo;
    private IndexStatusRepository statusRepo;
    private DocumentRepository docRepo;

    public IndexCreatedHandlerTest() throws Exception
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
        docRepo = new DocumentRepository(f.getSession());
    }

    @After
    public void tearDown()
    {
    }

    /**
     * Test of handles method, of class IndexCreatedHandler.
     */
    @Test
    public void testHandles()
    {
        System.out.println("handles");
        Class eventClass = IndexCreatedEvent.class;
        IndexCreatedHandler instance = new IndexCreatedHandler(indexRepo, statusRepo, docRepo);
        boolean result = instance.handles(eventClass);
        assertEquals(true, result);
        Object o = new Object();
        result = instance.handles(o.getClass());
        assertEquals(false, result);
    }

    /**
     * Test of handle method, of class IndexCreatedHandler.
     */
    @Test
    public void testHandle() throws Exception
    {
        System.out.println("handle");
        //datasetup
        Index testIndex = Fixtures.createTestIndexWithBulkDataHit();
        f.insertIndex(testIndex);
        IndexCreatedEvent entity = Fixtures.createTestIndexCreationStatusWithBulkDataHit();
        entity.setTotalRecords(34);
        statusRepo.createEntity(entity);
        Object event = entity;
        //end data setup
        IndexCreatedHandler instance = new IndexCreatedHandler(indexRepo, statusRepo, docRepo);
        //call
        instance.handle(event);
        //verify
        assertTrue(statusRepo.exists(entity.getUuid()));
        IndexCreatedEvent storedStatus = statusRepo.readEntityByUUID(entity.getUuid());
        assertNotNull(storedStatus);
        assertTrue(storedStatus.isDoneIndexing());
        assertEquals(storedStatus.getTotalRecords(), storedStatus.getRecordsCompleted());
        assertEquals(100, storedStatus.getPrecentComplete(), 0);
        assertEquals(storedStatus.getEta(), 0);
        assertNotEquals(storedStatus.getDateStarted(), storedStatus.getStatusLastUpdatedAt());
        assertTrue(statusRepo.readAllCurrentlyIndexing().isEmpty());
        assertNotNull(storedStatus.getIndex());
        Index readIndex = indexRepo.readEntityById(testIndex.getId());
        assertNotNull(readIndex);
        assertTrue(readIndex.isActive());
    }

    /**
     * Test of handle method, of class IndexCreatedHandler.
     */
    @Test
    public void testHandleWithError() throws Exception
    {
        System.out.println("handleWithError");
        //datasetup
        //Index testIndex = Fixtures.createTestIndexWithBulkDataHit();
        //f.insertIndex(testIndex);//no index associated with this status; not likley to happen, but easy way to cause an exception
        IndexCreatedEvent entity = Fixtures.createTestIndexCreationStatusWithBulkDataHit();
        entity.setTotalRecords(34);
        statusRepo.createEntity(entity);
        Object event = entity;
        //end data setup
        IndexCreatedHandler instance = new IndexCreatedHandler(indexRepo, statusRepo, docRepo);
        //call
        boolean expectedExceptionThrown = false;
        try
        {
            instance.handle(event);
        } catch (Exception e)
        {
            expectedExceptionThrown = true;
        }
        assertTrue("Expected exception not thrown.", expectedExceptionThrown);
        //verify
        assertTrue(statusRepo.exists(entity.getUuid()));
        IndexCreatedEvent storedStatus = statusRepo.readEntityByUUID(entity.getUuid());
        assertNotNull(storedStatus);
        assertFalse(storedStatus.isDoneIndexing());
        assertNotEquals(storedStatus.getTotalRecords(), storedStatus.getRecordsCompleted());
        assertEquals(0, storedStatus.getPrecentComplete(), 0);
        assertEquals(storedStatus.getEta(), -1);
        assertNotEquals(storedStatus.getDateStarted(), storedStatus.getStatusLastUpdatedAt());
        assertFalse(statusRepo.readAllCurrentlyIndexing().isEmpty());
        assertEquals("Could not complete indexing event for index: 'myindexbulkdata'. Please contact a system administrator to resolve this issue.", storedStatus.getFatalError());
    }

    /**
     * Test of handle method, of class IndexCreatedHandler.
     */
    @Test
    public void testHandleWithData() throws Exception
    {
        System.out.println("handleWithData");
        //datasetup
        //insert test docs and stuff
        Database testDb = Fixtures.createTestPlayersDatabase();
        Table testTable = Fixtures.createTestPlayersTable();
        f.insertDatabase(testDb);
        f.insertTable(testTable);
        List<Document> docs = Fixtures.getBulkDocuments("./src/test/resources/players-short.json", testTable);
        f.insertDocuments(docs);//put in a ton of data directly into the db

        //insert index
        Index lastname = Fixtures.createTestPlayersIndexLastName();
        f.insertIndex(lastname);

        IndexCreatedEvent entity = new IndexCreatedEvent(UUID.randomUUID(), new Date(), new Date(), lastname, docs.size(), 0);

        statusRepo.createEntity(entity);
        Object event = entity;
        //end data setup
        IndexCreatedHandler instance = new IndexCreatedHandler(indexRepo, statusRepo, docRepo);
        //call
        instance.handle(event);
        //verify
        assertTrue(statusRepo.exists(entity.getUuid()));
        IndexCreatedEvent storedStatus = statusRepo.readEntityByUUID(entity.getUuid());
        assertNotNull(storedStatus);
        assertTrue(storedStatus.isDoneIndexing());
        assertEquals(storedStatus.getTotalRecords(), storedStatus.getRecordsCompleted());
        assertEquals(100, storedStatus.getPrecentComplete(), 0);
        assertEquals(storedStatus.getEta(), 0);
        assertNotEquals(storedStatus.getDateStarted(), storedStatus.getStatusLastUpdatedAt());
        assertTrue(statusRepo.readAllCurrentlyIndexing().isEmpty());
        assertNotNull(storedStatus.getIndex());
        Index readIndex = indexRepo.readEntityById(lastname.getId());
        assertNotNull(readIndex);
        assertTrue(readIndex.isActive());
    }
}

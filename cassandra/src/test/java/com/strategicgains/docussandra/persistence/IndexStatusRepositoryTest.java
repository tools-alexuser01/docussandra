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
package com.strategicgains.docussandra.persistence;

import com.strategicgains.docussandra.Utils;
import com.strategicgains.docussandra.cache.CacheFactory;
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.event.IndexCreatedEvent;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Not super-happy with this test right now.
 *
 * @author udeyoje
 */
public class IndexStatusRepositoryTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexStatusRepositoryTest.class);
    private static Fixtures f;

    public IndexStatusRepositoryTest() throws Exception
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
        f.clearTestTables();
    }

    @Before
    public void setUp() throws IOException, InterruptedException
    {
        Utils.initDatabase("/docussandra.cql", f.getSession());//hard clear of the test tables
        Thread.sleep(5000);
        CacheFactory.clearAllCaches();
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        //it was passing before with out the next lines; HOW?
        f.insertTable(Fixtures.createTestTable());
        f.insertIndex(Fixtures.createTestIndexAllFieldTypes());
        f.insertIndex(Fixtures.createTestIndexOneField());
    }

    @After
    public void tearDown()
    {
    }

    /**
     * Test of exists method, of class IndexStatusRepository.
     */
    @Test
    public void testExists_UUID()
    {
        System.out.println("exists");
        IndexCreatedEvent entity = Fixtures.createTestIndexCreationStatus();
        IndexStatusRepository instance = new IndexStatusRepository(f.getSession());
        UUID id = entity.getUuid();
        boolean result = instance.exists(id);
        assertEquals(false, result);
        instance.createEntity(entity);
        result = instance.exists(id);
        assertEquals(true, result);
    }

    /**
     * Test of createEntity method, of class IndexStatusRepository.
     */
    @Test
    public void testCreateEntity()
    {
        System.out.println("createEntity");
        IndexCreatedEvent entity = Fixtures.createTestIndexCreationStatus();
        IndexStatusRepository instance = new IndexStatusRepository(f.getSession());
        IndexCreatedEvent result = instance.createEntity(entity);
        assertEquals(entity, result);
    }

    /**
     * Test of updateEntity method, of class IndexStatusRepository.
     */
    @Test
    public void testUpdateEntity()
    {
        System.out.println("updateEntity");
        IndexCreatedEvent entity = Fixtures.createTestIndexCreationStatus();
        IndexStatusRepository instance = new IndexStatusRepository(f.getSession());
        //create
        instance.createEntity(entity);
        //update
        entity.setRecordsCompleted(10);
        entity.setStatusLastUpdatedAt(new Date());
        IndexCreatedEvent result = instance.updateEntity(entity);
        assertStatusEqualEnough(entity, result);
        //fetch
        IndexCreatedEvent read = instance.readEntityByUUID(entity.getUuid());
        assertStatusEqualEnough(entity, read);
    }

    /**
     * Test of updateEntity method, of class IndexStatusRepository.
     */
    @Test
    public void testUpdateEntityWithFatalErrorField()
    {
        System.out.println("updateEntityWithFatalErrorField");
        IndexCreatedEvent entity = Fixtures.createTestIndexCreationStatus();
        IndexStatusRepository instance = new IndexStatusRepository(f.getSession());
        //create
        instance.createEntity(entity);
        //update
        entity.setRecordsCompleted(10);
        entity.setStatusLastUpdatedAt(new Date());
        entity.setFatalError("Whoops! Something Went Wrong.");
        IndexCreatedEvent result = instance.updateEntity(entity);
        assertStatusEqualEnough(entity, result);
        //fetch
        IndexCreatedEvent read = instance.readEntityByUUID(entity.getUuid());
        assertStatusEqualEnough(entity, read);
    }

    /**
     * Test of updateEntity method, of class IndexStatusRepository.
     */
    @Test
    public void testUpdateEntityWithNullFatalErrorField()
    {
        System.out.println("updateEntityWithNullFatalErrorField");
        IndexCreatedEvent entity = Fixtures.createTestIndexCreationStatus();
        IndexStatusRepository instance = new IndexStatusRepository(f.getSession());
        //create
        instance.createEntity(entity);
        //update
        entity.setRecordsCompleted(10);
        entity.setStatusLastUpdatedAt(new Date());
        entity.setFatalError(null);
        IndexCreatedEvent result = instance.updateEntity(entity);
        assertStatusEqualEnough(entity, result);
        //fetch
        IndexCreatedEvent read = instance.readEntityByUUID(entity.getUuid());
        assertStatusEqualEnough(entity, read);
    }

    /**
     * Test of updateEntity method, of class IndexStatusRepository.
     */
    @Test
    public void testUpdateEntityWithErrorsField()
    {
        System.out.println("updateEntityWithErrorsField");
        IndexCreatedEvent entity = Fixtures.createTestIndexCreationStatus();
        IndexStatusRepository instance = new IndexStatusRepository(f.getSession());
        //create
        instance.createEntity(entity);
        //update
        entity.setRecordsCompleted(10);
        entity.setStatusLastUpdatedAt(new Date());
        ArrayList<String> errors = new ArrayList<>();
        errors.add("Whoops! Something Went Wrong.");
        entity.setErrors(errors);
        IndexCreatedEvent result = instance.updateEntity(entity);
        assertStatusEqualEnough(entity, result);
        //fetch
        IndexCreatedEvent read = instance.readEntityByUUID(entity.getUuid());
        assertStatusEqualEnough(entity, read);
    }

    /**
     * Test of updateEntity method, of class IndexStatusRepository.
     */
    @Test
    public void testUpdateEntityWithNullErrorsField()
    {
        System.out.println("updateEntityWithNullErrorsField");
        IndexCreatedEvent entity = Fixtures.createTestIndexCreationStatus();
        IndexStatusRepository instance = new IndexStatusRepository(f.getSession());
        //create
        instance.createEntity(entity);
        //update
        entity.setRecordsCompleted(10);
        entity.setStatusLastUpdatedAt(new Date());
        entity.setErrors(null);
        IndexCreatedEvent result = instance.updateEntity(entity);
        assertStatusEqualEnough(entity, result);
        //fetch
        IndexCreatedEvent read = instance.readEntityByUUID(entity.getUuid());
        assertStatusEqualEnough(entity, read);
    }

//not implemented
//    /**
//     * Test of deleteEntity method, of class IndexStatusRepository.
//     */
//    @Test
//    public void testDeleteEntity()
//    {
//        System.out.println("deleteEntity");
//        IndexCreatedEvent id = null;
//        IndexStatusRepository instance = null;
//        instance.deleteEntity(id);
//        fail("The test case is a prototype.");
//    }
    /**
     * Test of readAll method, of class IndexStatusRepository.
     */
    @Test
    public void testReadAll()
    {
        System.out.println("readAll");
        IndexCreatedEvent entity = Fixtures.createTestIndexCreationStatus();
        IndexStatusRepository instance = new IndexStatusRepository(f.getSession());

        List<IndexCreatedEvent> result = instance.readAll();
        assertTrue(result.isEmpty());
        //create        
        instance.createEntity(entity);
        //re-read        
        result = instance.readAll();
        assertStatusEqualEnough(entity, result.get(0));
        //create again  
        IndexCreatedEvent entity2 = Fixtures.createTestIndexCreationStatus();
        entity2.setUuid(UUID.randomUUID());
        instance.createEntity(entity2);
        //re-read again   
        result = instance.readAll();
        assertTrue(result.size() == 2);
    }

//not implemented    
//    /**
//     * Test of countAll method, of class IndexStatusRepository.
//     */
//    @Test
//    public void testCountAll()
//    {
//        System.out.println("countAll");
//        String namespace = "";
//        String collection = "";
//        IndexStatusRepository instance = null;
//        long expResult = 0L;
//        long result = instance.countAll(namespace, collection);
//        assertEquals(expResult, result);
//        fail("The test case is a prototype.");
//    }
    /**
     * Test of readEntityByUUID method, of class IndexStatusRepository.
     */
    @Test
    public void testReadEntityByUUID()
    {
        System.out.println("readEntityByUUID");
        IndexCreatedEvent entity = Fixtures.createTestIndexCreationStatus();
        IndexStatusRepository instance = new IndexStatusRepository(f.getSession());
        UUID id = entity.getUuid();
        IndexCreatedEvent result = instance.readEntityByUUID(id);
        assertNull(result);
        instance.createEntity(entity);
        result = instance.readEntityByUUID(id);
        assertStatusEqualEnough(entity, result);
    }

    /**
     * Test of readAllCurrentlyIndexing method, of class IndexStatusRepository.
     */
    @Test
    public void testReadAllActive()
    {
        System.out.println("readAllActive");
        IndexCreatedEvent entity = Fixtures.createTestIndexCreationStatus();
        IndexStatusRepository instance = new IndexStatusRepository(f.getSession());
        instance.createEntity(entity);
        List<IndexCreatedEvent> result = instance.readAllCurrentlyIndexing();
        assertEquals(entity.getDateStarted(), result.get(0).getDateStarted());
        assertEquals(entity.getRecordsCompleted(), result.get(0).getRecordsCompleted());
        assertEquals(entity.getId(), result.get(0).getId());
        assertEquals(entity.getUuid(), result.get(0).getUuid());
        Index entityIndex = entity.getIndex();
        entityIndex.setActive(true);//simulate this index finishing
        entity.setIndex(entityIndex);
        instance.updateEntity(entity);
        result = instance.readAllCurrentlyIndexing();
        assertTrue(result.isEmpty());
    }

//    /**
//     * Test of initialize method, of class IndexStatusRepository.
//     */
//    @Test
//    public void testInitialize()
//    {
//        System.out.println("initialize");
//        IndexStatusRepository instance = null;
//        instance.initialize();
//        fail("The test case is a prototype.");
//    }
//    /**
//     * Test of updateEntityPkChange method, of class IndexStatusRepository.
//     */
//    @Test
//    public void testUpdateEntityPkChange()
//    {
//        System.out.println("updateEntityPkChange");
//        IndexCreatedEvent entity = null;
//        IndexStatusRepository instance = null;
//        IndexCreatedEvent expResult = null;
//        IndexCreatedEvent result = instance.updateEntityPkChange(entity);
//        assertEquals(expResult, result);
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of bindUUIDWhere method, of class IndexStatusRepository.
//     */
//    @Test
//    public void testBindUUIDWhere()
//    {
//        System.out.println("bindUUIDWhere");
//        BoundStatement bs = null;
//        UUID uuid = null;
//        IndexStatusRepository instance = null;
//        instance.bindUUIDWhere(bs, uuid);
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of marshalRow method, of class IndexStatusRepository.
//     */
//    @Test
//    public void testMarshalRow()
//    {
//        System.out.println("marshalRow");
//        Row row = null;
//        IndexStatusRepository instance = null;
//        IndexCreatedEvent expResult = null;
//        IndexCreatedEvent result = instance.marshalRow(row);
//        assertEquals(expResult, result);
//        fail("The test case is a prototype.");
//    }
//    /**
//     * Test of markDone method, of class IndexStatusRepository.
//     */
//    @Test
//    public void testMarkDone()
//    {
//        System.out.println("markDone");
//        UUID id = null;
//        IndexStatusRepository instance = null;
//        instance.markDone(id);
//        fail("The test case is a prototype.");
//    }
    private void assertStatusEqualEnough(IndexCreatedEvent expected, IndexCreatedEvent actual)
    {
        assertEquals(expected.getDateStarted(), actual.getDateStarted());
        assertEquals(expected.getRecordsCompleted(), actual.getRecordsCompleted());
        assertEquals(expected.getStatusLastUpdatedAt(), actual.getStatusLastUpdatedAt());
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getUuid(), actual.getUuid());
        assertEquals(expected.getFatalError(), actual.getFatalError());
    }

}

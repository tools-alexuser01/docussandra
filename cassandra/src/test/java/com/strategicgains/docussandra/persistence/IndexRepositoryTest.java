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

import com.datastax.driver.core.Row;
import com.strategicgains.docussandra.Utils;
import com.strategicgains.docussandra.cache.CacheFactory;
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.testhelper.Fixtures;
import com.strategicgains.repoexpress.domain.Identifier;
import java.io.IOException;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author udeyoje
 */
public class IndexRepositoryTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexRepositoryTest.class);
    private static Fixtures f;
    private Table testTable = Fixtures.createTestTable();

    public IndexRepositoryTest() throws Exception
    {
        f = Fixtures.getInstance(true);
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
        f.insertTable(testTable);
    }

    /**
     * Test of exists method, of class IndexRepository.
     */
    @Test
    public void testExists()
    {
        System.out.println("exists");
        Index testIndex = Fixtures.createTestIndexOneField();
        Identifier identifier = testIndex.getId();
        IndexRepository instance = new IndexRepository(f.getSession());
        boolean result = instance.exists(identifier);
        assertEquals(false, result);
        f.insertIndex(testIndex);
        result = instance.exists(identifier);
        assertEquals(true, result);
    }

    /**
     * Test of readEntityById method, of class IndexRepository.
     */
    @Test
    public void testReadEntityById()
    {
        System.out.println("readEntityById");
        Index testIndex = Fixtures.createTestIndexOneField();
        Identifier identifier = testIndex.getId();
        IndexRepository instance = new IndexRepository(f.getSession());
        Index result = instance.readEntityById(identifier);
        assertNull(result);
        f.insertIndex(testIndex);
        result = instance.readEntityById(identifier);
        assertEquals(testIndex, result);
    }

    /**
     * Test of createEntity method, of class IndexRepository.
     */
    @Test
    public void testCreateEntity()
    {
        System.out.println("createEntity");
        Index entity = Fixtures.createTestIndexOneField();
        IndexRepository instance = new IndexRepository(f.getSession());
        Index expResult = entity;
        Index result = instance.createEntity(entity);
        assertEquals(expResult, result);
        result = instance.readEntityById(result.getId());
        assertEquals(expResult, result);
    }

    /**
     * Test of markActive method, of class IndexRepository.
     */
    @Test
    public void testMarkActive()
    {
        System.out.println("markActive");
        Index entity = Fixtures.createTestIndexOneField();
        f.insertIndex(entity);
        IndexRepository instance = new IndexRepository(f.getSession());
        Index result = instance.readEntityById(entity.getId());
        assertFalse(result.isActive());
        instance.markActive(entity);
        result = instance.readEntityById(entity.getId());
        assertTrue(result.isActive());
    }

    /**
     * Test of updateEntity method, of class IndexRepository.
     */
    @Test
    public void testUpdateEntity()
    {
        System.out.println("updateEntity");
        IndexRepository instance = new IndexRepository(f.getSession());
        boolean expectedExceptionThrown = false;
        try
        {
            Index result = instance.updateEntity(Fixtures.createTestIndexOneField());
        } catch (UnsupportedOperationException e)
        {
            expectedExceptionThrown = true;
        }
        assertTrue(expectedExceptionThrown);
    }

    /**
     * Test of deleteEntity method, of class IndexRepository.
     */
    @Test
    public void testDeleteEntity()
    {
        System.out.println("deleteEntity");
        Index entity = Fixtures.createTestIndexOneField();
        f.insertIndex(entity);
        IndexRepository instance = new IndexRepository(f.getSession());
        instance.deleteEntity(entity);
        assertNull(instance.readEntityById(entity.getId()));
    }

    /**
     * Test of deleteEntity method, of class IndexRepository.
     */
    @Test
    public void testDeleteEntityWithDeleteCascade() throws InterruptedException
    {
        System.out.println("deleteEntityWithDeleteCascade");
        //setup
        f.insertIndex(Fixtures.createTestIndexOneField());
        //act
        IndexRepository indexRepo = new IndexRepository(f.getSession());
        indexRepo.delete(Fixtures.createTestIndexOneField());
        Thread.sleep(5000);
        //check index deletion        
        assertFalse(indexRepo.exists(Fixtures.createTestIndexOneField().getId()));
        //check iTable deletion
        ITableRepository iTableRepo = new ITableRepository(f.getSession());
        assertFalse(iTableRepo.iTableExists(Fixtures.createTestIndexOneField()));
    }

    /**
     * Test of readAll method, of class IndexRepository.
     */
    @Test
    public void testReadAll()
    {
        System.out.println("readAll");
        Index testIndex = Fixtures.createTestIndexOneField();
        Index testIndex2 = Fixtures.createTestIndexTwoField();
        f.insertIndex(testIndex);
        f.insertIndex(testIndex2);
        String namespace = testIndex.getDatabaseName();
        String collection = testIndex.getTableName();
        IndexRepository instance = new IndexRepository(f.getSession());
        List<Index> result = instance.readAll(namespace, collection);
        assertEquals(2, result.size());
        assertEquals(result.get(0), testIndex);
        assertEquals(result.get(1), testIndex2);

    }

    /**
     * Test of readAllCached method, of class IndexRepository.
     */
    @Test
    public void testReadAllCached()
    {
        System.out.println("readAllCached");
        Index testIndex = Fixtures.createTestIndexOneField();
        Index testIndex2 = Fixtures.createTestIndexTwoField();
        f.insertIndex(testIndex);
        f.insertIndex(testIndex2);
        String namespace = testIndex.getDatabaseName();
        String collection = testIndex.getTableName();
        IndexRepository instance = new IndexRepository(f.getSession());
        List<Index> result = instance.readAllCached(namespace, collection);
        assertEquals(2, result.size());
        assertEquals(result.get(0), testIndex);
        assertEquals(result.get(1), testIndex2);
    }

    /**
     * Test of countAll method, of class IndexRepository.
     */
    @Test
    public void testCountAll()
    {
        System.out.println("countAll");
        Index testIndex = Fixtures.createTestIndexOneField();
        Index testIndex2 = Fixtures.createTestIndexTwoField();
        f.insertIndex(testIndex);
        f.insertIndex(testIndex2);
        String namespace = testIndex.getDatabaseName();
        String collection = testIndex.getTableName();
        IndexRepository instance = new IndexRepository(f.getSession());
        long expResult = 2L;
        long result = instance.countAll(namespace, collection);
        assertEquals(expResult, result);
    }

}

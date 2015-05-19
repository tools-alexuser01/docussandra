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

import com.strategicgains.docussandra.persistence.impl.ITableRepositoryImpl;
import com.strategicgains.docussandra.Utils;
import com.strategicgains.docussandra.cache.CacheFactory;
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Identifier;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.exception.ItemNotFoundException;
import com.strategicgains.docussandra.testhelper.Fixtures;
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
        //Thread.sleep(5000);
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
     * Test of read method, of class IndexRepository.
     */
    @Test
    public void testReadById()
    {
        System.out.println("readById");
        Index testIndex = Fixtures.createTestIndexOneField();
        Identifier identifier = testIndex.getId();
        IndexRepository instance = new IndexRepository(f.getSession());
        boolean expectedExceptionThrown = false;
        try
        {
            instance.read(identifier);
        } catch (ItemNotFoundException e)
        {
            expectedExceptionThrown = true;
        }
        assertTrue("Expected exception not thrown", expectedExceptionThrown);
        f.insertIndex(testIndex);
        Index result = instance.read(identifier);
        assertEquals(testIndex, result);
    }

    /**
     * Test of create method, of class IndexRepository.
     */
    @Test
    public void testCreateEntity()
    {
        System.out.println("create");
        Index entity = Fixtures.createTestIndexOneField();
        IndexRepository instance = new IndexRepository(f.getSession());
        Index expResult = entity;
        Index result = instance.create(entity);
        assertEquals(expResult, result);
        result = instance.read(result.getId());
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
        Index result = instance.read(entity.getId());
        assertFalse(result.isActive());
        instance.markActive(entity);
        result = instance.read(entity.getId());
        assertTrue(result.isActive());
    }

    /**
     * Test of update method, of class IndexRepository.
     */
    @Test
    public void testUpdate()
    {
        System.out.println("update");
        IndexRepository instance = new IndexRepository(f.getSession());
        boolean expectedExceptionThrown = false;
        try
        {
            Index result = instance.update(Fixtures.createTestIndexOneField());
        } catch (UnsupportedOperationException e)
        {
            expectedExceptionThrown = true;
        }
        assertTrue(expectedExceptionThrown);
    }

    /**
     * Test of delete method, of class IndexRepository.
     */
    @Test
    public void testDelete()
    {
        System.out.println("delete");
        Index entity = Fixtures.createTestIndexOneField();
        f.insertIndex(entity);
        IndexRepository instance = new IndexRepository(f.getSession());
        instance.delete(entity);
        boolean expectedExceptionThrown = false;
        try
        {
            instance.read(entity.getId());
        } catch (ItemNotFoundException e)
        {
            expectedExceptionThrown = true;
        }
        assertTrue("Expected exception not thrown", expectedExceptionThrown);
    }

    /**
     * Test of delete method, of class IndexRepository.
     */
    @Test
    public void testDeleteById()
    {
        System.out.println("deleteById");
        Index entity = Fixtures.createTestIndexOneField();
        f.insertIndex(entity);
        IndexRepository instance = new IndexRepository(f.getSession());
        instance.delete(entity.getId());
        boolean expectedExceptionThrown = false;
        try
        {
            instance.read(entity.getId());
        } catch (ItemNotFoundException e)
        {
            expectedExceptionThrown = true;
        }
        assertTrue("Expected exception not thrown", expectedExceptionThrown);
    }

    /**
     * Test of delete method, of class IndexRepository.
     */
    @Test
    public void testDeleteWithDeleteCascade() throws InterruptedException
    {
        System.out.println("deleteWithDeleteCascade");
        //setup
        f.insertIndex(Fixtures.createTestIndexOneField());
        //act
        IndexRepository indexRepo = new IndexRepository(f.getSession());
        indexRepo.delete(Fixtures.createTestIndexOneField());
        //Thread.sleep(5000);
        //check index deletion        
        assertFalse(indexRepo.exists(Fixtures.createTestIndexOneField().getId()));
        //check iTable deletion
        ITableRepository iTableRepo = new ITableRepositoryImpl(f.getSession());
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
        String database = testIndex.getDatabaseName();
        String table = testIndex.getTableName();
        IndexRepository instance = new IndexRepository(f.getSession());
        List<Index> result = instance.readAll(new Identifier(database, table));
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
        String database = testIndex.getDatabaseName();
        String table = testIndex.getTableName();
        IndexRepository instance = new IndexRepository(f.getSession());
        List<Index> result = instance.readAllCached(new Identifier(database, table));
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
        String database = testIndex.getDatabaseName();
        String table = testIndex.getTableName();
        IndexRepository instance = new IndexRepository(f.getSession());
        long expResult = 2L;
        long result = instance.countAll(new Identifier(database, table));
        assertEquals(expResult, result);
    }

}

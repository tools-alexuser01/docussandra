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
import com.strategicgains.docussandra.cache.CacheFactory;
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.testhelper.Fixtures;
import com.strategicgains.repoexpress.domain.Identifier;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for the table repo
 *
 * @author udeyoje
 */
public class TableRepositoryTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger(TableRepositoryTest.class);
    private Fixtures f;

    public TableRepositoryTest() throws Exception
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
    public void setUp()
    {
        f.clearTestTables();
        CacheFactory.clearAllCaches();
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
    }

    @After
    public void tearDown()
    {
        f.clearTestTables();
    }

    /**
     * Test of exists method, of class TableRepository.
     */
    @Test
    public void testExists()
    {
        System.out.println("exists");
        Table testTable = Fixtures.createTestTable();
        Identifier identifier = testTable.getId();
        TableRepository instance = new TableRepository(f.getSession());
        boolean result = instance.exists(identifier);
        assertEquals(false, result);
        f.insertTable(testTable);
        result = instance.exists(identifier);
        assertEquals(true, result);
    }

    /**
     * Test of readEntityById method, of class TableRepository.
     */
    @Test
    public void testReadEntityById()
    {
        System.out.println("readEntityById");
        Table testTable = Fixtures.createTestTable();
        Identifier identifier = testTable.getId();
        TableRepository instance = new TableRepository(f.getSession());
        f.insertTable(testTable);
        Table result = instance.read(identifier);
        assertNotNull(result);
        assertEquals(testTable, result);
    }

    /**
     * Test of createEntity method, of class TableRepository.
     */
    @Test
    public void testCreateEntity()
    {
        System.out.println("createEntity");
        Table entity = Fixtures.createTestTable();
        TableRepository instance = new TableRepository(f.getSession());
        Table result = instance.createEntity(entity);
        assertEquals(entity, result);

    }

    /**
     * Test of updateEntity method, of class TableRepository.
     */
    @Test
    public void testUpdateEntity()
    {
        System.out.println("updateEntity");
        Table entity = Fixtures.createTestTable();
        TableRepository instance = new TableRepository(f.getSession());
        Table created = instance.createEntity(entity);
        assertEquals(entity, created);
        String newDesciption = "this is a new description";
        created.description(newDesciption);
        Table result = instance.updateEntity(entity);
        assertEquals(created, result);
        result.name("new_name1");
        Table resultNew = instance.updateEntity(entity);
        assertEquals(result, resultNew);
        instance.delete(resultNew.getId());
    }

    /**
     * Test of deleteEntity method, of class TableRepository.
     */
    @Test
    public void testDeleteEntity()
    {
        System.out.println("deleteEntity");
        Table testTable = Fixtures.createTestTable();
        Identifier identifier = testTable.getId();
        f.insertTable(testTable);
        TableRepository instance = new TableRepository(f.getSession());
        boolean result = instance.exists(identifier);
        assertEquals(true, result);
        instance.delete(identifier);
        result = instance.exists(identifier);
        assertEquals(false, result);
    }

    /**
     * Test of readAll method, of class TableRepository.
     */
    @Test
    public void testReadAll()
    {
        System.out.println("readAll");
        Table testTable = Fixtures.createTestTable();
        f.insertTable(testTable);
        String namespace = testTable.databaseName();
        TableRepository instance = new TableRepository(f.getSession());
        List<Table> expResult = new ArrayList<>();
        expResult.add(testTable);
        List<Table> result = instance.readAll(namespace);
        assertEquals(expResult, result);
    }

    /**
     * Test of countAllTables method, of class TableRepository.
     */
    @Test
    public void testCountAllTables()
    {
        System.out.println("countAllTables");
        TableRepository instance = new TableRepository(f.getSession());
        Table testTable = Fixtures.createTestTable();
        String namespace = testTable.databaseName();
        long result = instance.countAllTables(namespace);
        long expResult = 0L;
        assertEquals(expResult, result);
        f.insertTable(testTable);
        expResult = 1L;
        result = instance.countAllTables(namespace);
        assertEquals(expResult, result);
    }

    /**
     * Test of countTableSize method, of class TableRepository.
     */
    @Test
    public void testCountTableSize()
    {
        System.out.println("countTableSize");
        TableRepository instance = new TableRepository(f.getSession());
        Table testTable = Fixtures.createTestTable();
        String namespace = testTable.databaseName();
        String tableName = testTable.name();
        f.insertTable(testTable);
        long expResult = 0L;
        long result = instance.countTableSize(namespace, tableName);
        assertEquals(expResult, result);

        f.insertDocument(Fixtures.createTestDocument());
        expResult = 1L;
        result = instance.countTableSize(namespace, tableName);
        assertEquals(expResult, result);

        f.insertDocument(Fixtures.createTestDocument2());
        expResult = 2L;
        result = instance.countTableSize(namespace, tableName);
        assertEquals(expResult, result);
    }

}

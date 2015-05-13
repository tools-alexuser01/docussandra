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

import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.testhelper.Fixtures;
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
 *
 * @author udeyoje
 */
public class DatabaseRepositoryTest
{
    
    private static Fixtures f;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseRepositoryTest.class);
    
    public DatabaseRepositoryTest() throws Exception
    {
        f = Fixtures.getInstance();
    }
    
    @BeforeClass
    public static void setUpClass()
    {
    }
    
    @AfterClass
    public static void tearDownClass()
    {
        f.clearTestTables();// clear anything that might be there already
    }
    
    @Before
    public void setUp()
    {
        f.clearTestTables();// clear anything that might be there already
    }
    
    @After
    public void tearDown()
    {
        
    }

    /**
     * Test of createEntity method, of class DatabaseRepository.
     */
    @Test
    public void testCreateEntity()
    {
        System.out.println("createEntity");
        Database entity = Fixtures.createTestDatabase();
        DatabaseRepository instance = new DatabaseRepository(f.getSession());
        Database result = instance.createEntity(entity);
        assertEquals(entity, result);
    }

    /**
     * Test of updateEntity method, of class DatabaseRepository.
     */
    @Test
    public void testUpdateEntity()
    {
        System.out.println("updateEntity");
        //setup
        Database entity = Fixtures.createTestDatabase();
        f.insertDatabase(entity);
        //act
        DatabaseRepository instance = new DatabaseRepository(f.getSession());
        entity.description("This is a new description!");
        Database result = instance.updateEntity(entity);
        //assert
        assertEquals(entity, result);
    }

    /**
     * Test of deleteEntity method, of class DatabaseRepository.
     */
    @Test
    public void testDeleteEntity()
    {
        System.out.println("deleteEntity");
        //setup
        Database entity = Fixtures.createTestDatabase();
        f.insertDatabase(entity);
        //act
        DatabaseRepository instance = new DatabaseRepository(f.getSession());
        instance.deleteEntity(entity);
        //check
        List<Database> allRows = instance.readAll();
        assertFalse(allRows.contains(entity));
    }

    /**
     * Test of deleteEntity method, of class DatabaseRepository.
     */
    @Test
    public void testDeleteEntityWithDeleteCascade() throws InterruptedException
    {
        System.out.println("deleteEntityWithDeleteCascade");
        //setup
        final Database entity = Fixtures.createTestDatabase();
        f.insertDatabase(entity);
        f.insertTable(Fixtures.createTestTable());
        f.insertIndex(Fixtures.createTestIndexOneField());
        f.insertDocument(Fixtures.createTestDocument());
        //act
        DatabaseRepository instance = new DatabaseRepository(f.getSession());
        instance.delete(entity);
        Thread.sleep(5000);
        //check DB deletion
        List<Database> allRows = instance.readAll();
        assertFalse(allRows.contains(entity));
        //check table deletion
        TableRepository tableRepo = new TableRepository(f.getSession());
        assertFalse(tableRepo.exists(Fixtures.createTestTable().getId()));
        //check index deletion
        IndexRepository indexRepo = new IndexRepository(f.getSession());
        assertFalse(indexRepo.exists(Fixtures.createTestIndexOneField().getId()));
        //check iTable deletion
        ITableRepository iTableRepo = new ITableRepository(f.getSession());
        assertFalse(iTableRepo.iTableExists(Fixtures.createTestIndexOneField()));
        //check document deletion
        DocumentRepository docRepo = new DocumentRepository(f.getSession());
        boolean expectedExceptionThrown = false;
        try
        {
            docRepo.exists(Fixtures.createTestDocument().getId());
        } catch (Exception e)//should error because the entire table should no longer exist
        {
            assertTrue(e.getMessage().contains("unconfigured columnfamily"));
            assertTrue(e.getMessage().contains(Fixtures.createTestTable().toDbTable()));
            expectedExceptionThrown = true;
        }
        assertTrue(expectedExceptionThrown);
    }

    /**
     * Test of readAll method, of class DatabaseRepository.
     */
    @Test
    public void testReadAll()
    {
        System.out.println("readAll");
        //setup
        Database entity = Fixtures.createTestDatabase();
        f.insertDatabase(entity);
        //act
        DatabaseRepository instance = new DatabaseRepository(f.getSession());
        List<Database> result = instance.readAll();
        //check
        assertFalse(result.isEmpty());
        assertTrue(result.contains(entity));
    }
    
}

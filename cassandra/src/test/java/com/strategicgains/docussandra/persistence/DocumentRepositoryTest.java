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

import com.strategicgains.docussandra.cache.CacheFactory;
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Identifier;
import com.strategicgains.docussandra.domain.QueryResponseWrapper;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.exception.ItemNotFoundException;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.io.IOException;
import java.util.List;
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
 * Test for the document repository.
 *
 * @author udeyoje
 */
public class DocumentRepositoryTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentRepositoryTest.class);
    private static Fixtures f;
    private Database testDb;
    private Table testTable;

    public DocumentRepositoryTest()
    {
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        f = Fixtures.getInstance();
    }

    @AfterClass
    public static void tearDownClass()
    {
        f.clearTestTables();
    }

    @Before
    public void setUp()
    {
        f.clearTestTables();
        //clear all caches for the sake of this test, we will be createing and
        //deleting more frequently than a real world operation causing the cache
        //to hit no longer relevent objects
        CacheFactory.clearAllCaches();

        testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);

        testTable = Fixtures.createTestTable();
        f.insertTable(testTable);
    }

    @After
    public void tearDown()
    {
    }

    /**
     * Test of create method, of class DocumentRepository.
     */
    @Test
    public void testDoCreate()
    {
        System.out.println("doCreate");
        Document entity = Fixtures.createTestDocument();
        DocumentRepository instance = new DocumentRepository(f.getSession());
        Document result = instance.create(entity);
        assertNotNull(result);
        assertEquals(entity.databaseName(), result.databaseName());
        assertEquals(entity.tableName(), result.tableName());
        assertEquals(entity.object(), result.object());
        assertNotNull(result.getUuid());
        //cleanup the random uuid'ed doc
        f.deleteDocument(result);
    }

    /**
     * Test of read method, of class DocumentRepository.
     */
    @Test
    public void testDoRead()
    {
        System.out.println("doRead");
        //setup
        Document testDocument = Fixtures.createTestDocument();
        f.insertDocument(testDocument);

        Identifier identifier = testDocument.getId();
        DocumentRepository instance = new DocumentRepository(f.getSession());
        Document expResult = testDocument;
        Document result = instance.read(identifier);
        assertEquals(expResult, result);
        //cleanup the random uuid'ed doc
        f.deleteDocument(testDocument);
    }

    /**
     * Test of readAll method, of class DocumentRepository.
     */
    @Test
    public void testDoReadAll() throws IOException, ParseException
    {
        System.out.println("doReadAll");
        //setup        
        f.insertDocuments(Fixtures.getBulkDocuments());

        DocumentRepository instance = new DocumentRepository(f.getSession());

        List<Document> result = instance.readAll(testDb.name(), testTable.name(), 10, 0);
        assertTrue(result.size() == 10);
        assertNotNull(result.get(0).getId());
        assertNotNull(result.get(0).getUuid());
        assertNotNull(result.get(0).getUpdatedAt());
        result = instance.readAll(testDb.name(), testTable.name(), 10, 10);
        assertTrue(result.size() == 10);
        assertNotNull(result.get(0).getId());
        assertNotNull(result.get(0).getUuid());
        assertNotNull(result.get(0).getUpdatedAt());
        result = instance.readAll(testDb.name(), testTable.name(), 100, 20);
        assertTrue(result.size() == 14);
    }

    /*
     * Test of readAll method, of class DocumentRepository.
     */
    @Test
    public void testDoReadAll2() throws IOException, ParseException
    {
        System.out.println("doReadAll2");
        //setup        
        f.insertDocuments(Fixtures.getBulkDocuments());

        DocumentRepository instance = new DocumentRepository(f.getSession());
        
        //let's get the first 5
        QueryResponseWrapper result = instance.readAll(testDb.name(), testTable.name(), 5, 0);
        assertNotNull(result);
        assertTrue(!result.isEmpty());
        assertTrue(result.size() == 5);
        assertTrue(result.getNumAdditionalResults() == null);

        //now lets get the second 5
        result = instance.readAll(testDb.name(), testTable.name(), 5, 5);
        assertNotNull(result);
        assertTrue(!result.isEmpty());
        assertTrue(result.size() == 5);
        assertTrue(result.getNumAdditionalResults() == null);

        //now lets get the third 5
        result = instance.readAll(testDb.name(), testTable.name(), 5, 10);
        assertNotNull(result);
        assertTrue(!result.isEmpty());
        assertTrue(result.size() == 5);
        assertTrue(result.getNumAdditionalResults() == null);

        //now lets get the last 4
        result = instance.readAll(testDb.name(), testTable.name(), 5, 30);
        assertNotNull(result);
        assertTrue(!result.isEmpty());
        assertTrue(result.size() == 4);
        assertTrue(result.getNumAdditionalResults() == 0);
    }

    /**
     * Test of update method, of class DocumentRepository.
     */
    @Test
    public void testDoUpdate()
    {
        System.out.println("doUpdate");
        Document testDocument = Fixtures.createTestDocument();
        f.insertDocument(testDocument);
        DocumentRepository instance = new DocumentRepository(f.getSession());
        String newObject = "{\"newjson\": \"object\"}";
        testDocument.object(newObject);
        Document result = instance.update(testDocument);
        assertEquals(testDocument, result);
        //cleanup the random uuid'ed doc
        f.deleteDocument(testDocument);
    }

    /**
     * Test of delete method, of class DocumentRepository.
     */
    @Test
    public void testDoDelete()
    {
        System.out.println("doDelete");
        Document testDocument = Fixtures.createTestDocument();
        f.insertDocument(testDocument);
        DocumentRepository instance = new DocumentRepository(f.getSession());
        instance.delete(testDocument);
        boolean exceptionThrown = false;
        try
        {
            instance.read(testDocument.getId());
        } catch (ItemNotFoundException e)
        {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

    }

    /**
     * Test of several methods, of class DocumentRepository. Basically makes
     * sure our versioning doesn't screw up something unexpected.
     */
    @Test
    public void testVersioningLogic()
    {
        System.out.println("testVersioningLogic");
        DocumentRepository instance = new DocumentRepository(f.getSession());
        //create
        Document testDocument = Fixtures.createTestDocument();
        Document createResult = instance.create(testDocument);
        assertNotNull(createResult);
        //read create
        Document readResult = instance.read(createResult.getId());
        assertEquals(createResult, readResult);

        //update
        String newObject = "{\"newjson\": \"object\"}";
        testDocument.object(newObject);
        Document updateResult = instance.update(testDocument);
        assertEquals(testDocument, updateResult);

        //read update
        readResult = instance.read(createResult.getId());
        assertEquals(updateResult, readResult);

        //delete
        f.deleteDocument(testDocument);
        //make sure it is actually gone
        boolean exceptionThrown = false;
        try
        {
            instance.read(testDocument.getId());
        } catch (ItemNotFoundException e)
        {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        //create again
        testDocument = Fixtures.createTestDocument();
        createResult = instance.create(testDocument);
        assertNotNull(createResult);
        //read create again
        readResult = instance.read(createResult.getId());
        assertEquals(createResult, readResult);

        //update again
        newObject = "{\"newjson\": \"objectNew\"}";
        testDocument.object(newObject);
        updateResult = instance.update(testDocument);
        assertEquals(testDocument, updateResult);

        //read update again
        readResult = instance.read(createResult.getId());
        assertEquals(updateResult, readResult);

        //delete again
        f.deleteDocument(testDocument);
        //make sure it is actually gone
        exceptionThrown = false;
        try
        {
            instance.read(testDocument.getId());
        } catch (ItemNotFoundException e)
        {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    /**
     * Test of exists method, of class DocumentRepository.
     */
    @Test
    public void testExists()
    {
        System.out.println("exists");
        Document testDocument = Fixtures.createTestDocument();
        f.insertDocument(testDocument);
        DocumentRepository instance = new DocumentRepository(f.getSession());
        Identifier identifier = testDocument.getId();
        boolean result = instance.exists(identifier);
        assertEquals(true, result);
        f.deleteDocument(testDocument);
        result = instance.exists(identifier);
        assertEquals(false, result);
    }

}

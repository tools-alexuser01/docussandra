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

import com.datastax.driver.core.BoundStatement;
import com.strategicgains.docussandra.bucketmanagement.SimpleIndexBucketLocatorImpl;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.persistence.DocumentRepository;
import com.strategicgains.docussandra.persistence.IndexChangeObserver;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.TableRepository;
import com.strategicgains.docussandra.testhelper.Fixtures;
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
 *
 * @author udeyoje
 */
public class IndexMaintainerHelperTest
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private IndexRepository indexRepo;
    private DocumentRepository docRepo;
    private TableRepository tableRepo;
    //some test records
    private Index index1 = createTestIndexOneField();
    private Index index2 = createTestIndexTwoField();
    private Table table;

    private static Fixtures f;

    public IndexMaintainerHelperTest() throws Exception
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
        IndexChangeObserver ico = new IndexChangeObserver(f.getSession());
        indexRepo = new IndexRepository(f.getSession());
        docRepo = new DocumentRepository(f.getSession());
        tableRepo = new TableRepository(f.getSession());
        table = Fixtures.createTestTable();// new Table();
        f.clearTestTables();// clear anything that might be there already
        //f.createTestITables();
        //clearTestData();
        //reinsert with some fresh data
        index1 = Fixtures.createTestIndexOneField();
        index2 = Fixtures.createTestIndexTwoField();
        indexRepo.create(index1);
        indexRepo.create(index2);
    }

    @After
    public void tearDown()
    {
        
    }

    /**
     * Test of generateDocumentCreateIndexEntriesStatements method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateDocumentCreateIndexEntriesStatements()
    {
        System.out.println("generateDocumentCreateIndexEntriesStatements");
        Document entity = Fixtures.createTestDocument2();
        List<BoundStatement> result = IndexMaintainerHelper.generateDocumentCreateIndexEntriesStatements(f.getSession(), entity, new SimpleIndexBucketLocatorImpl());
        assertEquals(result.size(), 2);//one for each of our indices
        BoundStatement one = result.get(0);
        assertNotNull(one);
        for (int i = 0; i < 5; i++)
        {
            assertTrue(one.isSet(i));// 0 is the id, 1 is the blob, 2 and 3 are dates, 3 is the single index field for index1
        }
        assertEquals("docussandra", one.getKeyspace());
        assertEquals("INSERT INTO docussandra.mydb_mytable_myindexwithonefield (bucket, id, object, created_at, updated_at, myindexedfield) VALUES (?, ?, ?, ?, ?, ?);", one.preparedStatement().getQueryString());
        BoundStatement two = result.get(1);
        assertNotNull(two);
        for (int i = 0; i < 6; i++)
        {
            assertTrue(two.isSet(i));// 0 is the id, 1 is the blob, 2 and 3 are dates, 4 and 5 are the indexed fields for index2
        }
        assertEquals("docussandra", two.getKeyspace());
        assertEquals("INSERT INTO docussandra.mydb_mytable_myindexwithtwofields (bucket, id, object, created_at, updated_at, myindexedfield1,myindexedfield2) VALUES (?, ?, ?, ?, ?, ?, ?);", two.preparedStatement().getQueryString());
    }

    /**
     * Test of generateDocumentCreateIndexEntriesStatements method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateDocumentCreateIndexEntriesStatementsNoIndexField()
    {
        System.out.println("testGenerateDocumentCreateIndexEntriesStatementsNoIndexField");
        Document entity = Fixtures.createTestDocument2();
        entity.object("{}");//good luck indexing that!
        List<BoundStatement> result = IndexMaintainerHelper.generateDocumentCreateIndexEntriesStatements(f.getSession(), entity, new SimpleIndexBucketLocatorImpl());
        assertTrue(result.isEmpty());
    }

    /**
     * Test of generateDocumentUpdateIndexEntriesStatements method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateDocumentUpdateIndexEntriesStatements()
    {
        System.out.println("generateDocumentUpdateIndexEntriesStatements");
        Document entity = Fixtures.createTestDocument2();
        tableRepo.create(table);//create the table so we have a place to store the test data
        docRepo.doCreate(entity);//insert a document so we have something to reference
        List<BoundStatement> result = IndexMaintainerHelper.generateDocumentUpdateIndexEntriesStatements(f.getSession(), entity, new SimpleIndexBucketLocatorImpl());
        assertTrue(result.size() == 2);//one for each of our indices
        BoundStatement one = result.get(0);
        assertNotNull(one);
        for (int i = 0; i < 3; i++)
        {
            assertTrue(one.isSet(i));// 0 is the blob, 1 is the date, 2 is the UUID
        }
        assertEquals("docussandra", one.getKeyspace());
        assertEquals("UPDATE docussandra.mydb_mytable_myindexwithonefield SET object = ?, updated_at = ? WHERE bucket = ? AND myindexedfield = ?;", one.preparedStatement().getQueryString());
        BoundStatement two = result.get(1);
        assertNotNull(two);
        for (int i = 0; i < 4; i++)
        {
            assertTrue(two.isSet(i));// 0 is the blob, 1 is the date, 2 and 3 are indexed fields 
        }
        assertEquals("docussandra", two.getKeyspace());
        assertEquals("UPDATE docussandra.mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ? WHERE bucket = ? AND myindexedfield1 = ? AND myindexedfield2 = ?;", two.preparedStatement().getQueryString());
    }

    /**
     * Test of generateDocumentUpdateIndexEntriesStatements method, of class
     * IndexMaintainerHelper. This test includes functionality for when an
     * indexed field has changed.
     */
    @Test
    public void testGenerateDocumentUpdateIndexEntriesStatementsIndexChanged()
    {
        System.out.println("generateDocumentUpdateIndexEntriesStatementsIndexChanged");
        Document entity = Fixtures.createTestDocument2();
        tableRepo.create(table);//create the table so we have a place to store the test data
        docRepo.doCreate(entity);//insert a document so we have something to reference
        entity.object("{'greeting':'hello', 'myindexedfield': 'this is NOT my field', 'myindexedfield1':'my second field', 'myindexedfield2':'my third field'}");//change an indexed field
        List<BoundStatement> result = IndexMaintainerHelper.generateDocumentUpdateIndexEntriesStatements(f.getSession(), entity, new SimpleIndexBucketLocatorImpl());
        assertTrue(result.size() == 3);//one for the create, one for the delete, one for the second index

        //create
        BoundStatement one = result.get(0);
        assertNotNull(one);
        for (int i = 0; i < 5; i++)
        {
            assertTrue(one.isSet(i));// 0 is the id, 1 is the blob, 2 and 3 are dates, 3 is the single index field for index1
        }
        assertEquals("docussandra", one.getKeyspace());
        assertEquals("INSERT INTO docussandra.mydb_mytable_myindexwithonefield (bucket, id, object, created_at, updated_at, myindexedfield) VALUES (?, ?, ?, ?, ?, ?);", one.preparedStatement().getQueryString());
        //delete
        BoundStatement two = result.get(1);
        assertNotNull(one);
        assertTrue(two.isSet(0));//the UUID
        assertEquals("docussandra", two.getKeyspace());
        assertEquals("DELETE FROM docussandra.mydb_mytable_myindexwithonefield WHERE bucket = ? AND myindexedfield = ?;", two.preparedStatement().getQueryString());

        //the index update should proceed like a normal update
        BoundStatement three = result.get(2);
        assertNotNull(three);
        for (int i = 0; i < 4; i++)
        {
            assertTrue(three.isSet(i));// 0 is the blob, 1 is the date, 2 and 3 are indexed fields 
        }
        assertEquals("docussandra", three.getKeyspace());
        assertEquals("UPDATE docussandra.mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ? WHERE bucket = ? AND myindexedfield1 = ? AND myindexedfield2 = ?;", three.preparedStatement().getQueryString());
    }

    /**
     * Test of generateDocumentDeleteIndexEntriesStatements method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateDocumentDeleteIndexEntriesStatements()
    {
        System.out.println("generateDocumentDeleteIndexEntriesStatements");
        Document entity = Fixtures.createTestDocument2();
        entity.object("{'greeting':'hello', 'myindexedfield': 'this is my field', 'myindexedfield1':'my second field', 'myindexedfield2':'my third field'}");
        List<BoundStatement> result = IndexMaintainerHelper.generateDocumentDeleteIndexEntriesStatements(f.getSession(), entity, new SimpleIndexBucketLocatorImpl());
        assertEquals(result.size(), 2);//one for each of our indices
        BoundStatement one = result.get(0);
        assertNotNull(one);
        assertTrue(one.isSet(0));//the UUID
        assertEquals("docussandra", one.getKeyspace());
        assertEquals("DELETE FROM docussandra.mydb_mytable_myindexwithonefield WHERE bucket = ? AND myindexedfield = ?;", one.preparedStatement().getQueryString());
        BoundStatement two = result.get(1);
        assertNotNull(two);
        for (int i = 0; i < 1; i++)
        {
            assertTrue(two.isSet(i));// 0 and 1 are indexed fields 
        }
        assertEquals("docussandra", two.getKeyspace());
        assertEquals("DELETE FROM docussandra.mydb_mytable_myindexwithtwofields WHERE bucket = ? AND myindexedfield1 = ? AND myindexedfield2 = ?;", two.preparedStatement().getQueryString());
    }


    /**
     * Test of getIndexForDocument method, of class IndexMaintainerHelper.
     */
    @Test
    public void testGetIndexForDocument()
    {
        System.out.println("getIndexForDocument");
        Document entity = Fixtures.createTestDocument2();
        ArrayList<Index> exp = new ArrayList<>(2);
        exp.add(index1);
        exp.add(index2);
        List<Index> result = IndexMaintainerHelper.getIndexForDocument(f.getSession(), entity);
        assertNotNull(result);
        assertTrue(!result.isEmpty());
        assertTrue(result.size() == 2);
        assertEquals(exp, result);
    }

    /**
     * Test of generateCQLStatementForInsert method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateCQLStatementForInsert()
    {
        System.out.println("generateCQLStatementForInsert");
        String expResult = "INSERT INTO docussandra.mydb_mytable_myindexwithonefield (bucket, id, object, created_at, updated_at, myindexedfield) VALUES (?, ?, ?, ?, ?, ?);";
        String result = IndexMaintainerHelper.generateCQLStatementForInsert(index1);
        assertEquals(expResult, result);
        expResult = "INSERT INTO docussandra.mydb_mytable_myindexwithtwofields (bucket, id, object, created_at, updated_at, myindexedfield1,myindexedfield2) VALUES (?, ?, ?, ?, ?, ?, ?);";
        result = IndexMaintainerHelper.generateCQLStatementForInsert(index2);
        assertEquals(expResult, result);
    }

    /**
     * Test of generateCQLStatementForInsert method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateCQLStatementForInsert2()
    {
        System.out.println("generateCQLStatementForInsert");
        String expResult = "INSERT INTO docussandra.mydb_mytable_myindexwithonefield (bucket, id, object, created_at, updated_at, myindexedfield) VALUES (?, ?, ?, ?, ?, ?);";
        String result = IndexMaintainerHelper.getCQLStatementForInsert(index1);
        assertEquals(expResult, result);
        expResult = "INSERT INTO docussandra.mydb_mytable_myindexwithtwofields (bucket, id, object, created_at, updated_at, myindexedfield1,myindexedfield2) VALUES (?, ?, ?, ?, ?, ?, ?);";
        result = IndexMaintainerHelper.getCQLStatementForInsert(index2);
        assertEquals(expResult, result);
    }

    /**
     * Test of generateCQLStatementForWhereClauses method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateCQLStatementForUpdate()
    {
        System.out.println("generateCQLStatementForUpdate");
        String expResult = "UPDATE docussandra.mydb_mytable_myindexwithonefield SET object = ?, updated_at = ? WHERE bucket = ? AND myindexedfield = ?;";
        String result = IndexMaintainerHelper.generateCQLStatementForWhereClauses(IndexMaintainerHelper.ITABLE_UPDATE_CQL, index1);
        assertEquals(expResult, result);
        expResult = "UPDATE docussandra.mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ? WHERE bucket = ? AND myindexedfield1 = ? AND myindexedfield2 = ?;";
        result = IndexMaintainerHelper.generateCQLStatementForWhereClauses(IndexMaintainerHelper.ITABLE_UPDATE_CQL, index2);
        assertEquals(expResult, result);
    }

    /**
     * Test of generateCQLStatementForWhereClauses method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateCQLStatementForUpdate2()
    {
        System.out.println("generateCQLStatementForUpdate");
        String expResult = "UPDATE docussandra.mydb_mytable_myindexwithonefield SET object = ?, updated_at = ? WHERE bucket = ? AND myindexedfield = ?;";
        String result = IndexMaintainerHelper.getCQLStatementForWhereClauses(IndexMaintainerHelper.ITABLE_UPDATE_CQL, index1);
        assertEquals(expResult, result);
        expResult = "UPDATE docussandra.mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ? WHERE bucket = ? AND myindexedfield1 = ? AND myindexedfield2 = ?;";
        result = IndexMaintainerHelper.getCQLStatementForWhereClauses(IndexMaintainerHelper.ITABLE_UPDATE_CQL, index2);
        assertEquals(expResult, result);
    }

    /**
     * Test of hasIndexedFieldChanged method, of class IndexMaintainerHelper.
     */
    @Test
    public void testHasIndexedFieldChanged()
    {
        System.out.println("hasIndexedFieldChanged");
        tableRepo.create(table);//create the table so we have a place to store the test data
        Document entity = Fixtures.createTestDocument2();
        docRepo.doCreate(entity);//insert
        entity.object("{'greeting':'hola', 'myindexedfield': 'this is my field', 'myindexedfield1':'my second field', 'myindexedfield2':'my third field'}");//change a non-index field        
        boolean result = IndexMaintainerHelper.hasIndexedFieldChanged(f.getSession(), index1, entity);
        assertEquals(false, result);
        entity.object("{'greeting':'hello', 'myindexedfield': 'this is NOT my field', 'myindexedfield1':'my second field', 'myindexedfield2':'my third field'}");//change an indexed field
        result = IndexMaintainerHelper.hasIndexedFieldChanged(f.getSession(), index1, entity);
        assertEquals(true, result);
    }

    /**
     * Creates at test index with one field.
     *
     * @return
     */
    private static Index createTestIndexOneField()
    {
        Index index = new Index("myIndexWithOneField");
        index.table("mydb", "mytable");
        ArrayList<String> fields = new ArrayList<>();
        fields.add("myindexedfield");
        index.fields(fields);
        index.isUnique(false);
        return index;
    }

    /**
     * Creates at test index with two fields.
     *
     * @return
     */
    private static Index createTestIndexTwoField()
    {
        Index index = new Index("myIndexWithTwoFields");
        index.table("mydb", "mytable");
        ArrayList<String> fields = new ArrayList<>();
        fields.add("myindexedfield1");
        fields.add("myindexedfield2");
        index.fields(fields);
        index.isUnique(true);
        return index;
    }

}

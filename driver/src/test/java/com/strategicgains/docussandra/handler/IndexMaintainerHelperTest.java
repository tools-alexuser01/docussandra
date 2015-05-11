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
import com.strategicgains.docussandra.cache.CacheFactory;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.IndexField;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.exception.IndexParseException;
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
    private Index index1;
    private Index index2;
    private Index index3;
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
        CacheFactory.clearAllCaches();
    }

    @Before
    public void setUp()
    {
        IndexChangeObserver ico = new IndexChangeObserver(f.getSession());
        indexRepo = new IndexRepository(f.getSession());
        docRepo = new DocumentRepository(f.getSession());
        tableRepo = new TableRepository(f.getSession());
        CacheFactory.clearAllCaches();//clear the caches so we don't grab an old record that is no longer present
        table = Fixtures.createTestTable();// new Table();
        f.clearTestTables();// clear anything that might be there already
        //f.createTestITables();
        //clearTestData();
        //reinsert with some fresh data
        index1 = Fixtures.createTestIndexOneField();
        index2 = Fixtures.createTestIndexTwoField();
        index3 = Fixtures.createTestIndexAllFieldTypes();
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
    public void testGenerateDocumentCreateIndexEntriesStatements() throws IndexParseException
    {
        System.out.println("generateDocumentCreateIndexEntriesStatements");
        Document entity = Fixtures.createTestDocument2();
        List<BoundStatement> result = IndexMaintainerHelper.generateDocumentCreateIndexEntriesStatements(f.getSession(), entity, new SimpleIndexBucketLocatorImpl());
        assertEquals(result.size(), 2);//one for each of our indices
        BoundStatement one = result.get(0);
        assertNotNull(one);
        for (int i = 0; i < 5; i++)
        {
            assertTrue(one.isSet(i));// 0 is the id, 1 is the blob, 2 and 3 are dates, 4 is the single index field for index1
        }
        assertEquals("docussandra", one.getKeyspace());
        assertEquals("INSERT INTO mydb_mytable_myindexwithonefield (bucket, id, object, created_at, updated_at, myindexedfield) VALUES (?, ?, ?, ?, ?, ?);", one.preparedStatement().getQueryString());
        BoundStatement two = result.get(1);
        assertNotNull(two);
        for (int i = 0; i < 6; i++)
        {
            assertTrue(two.isSet(i));// 0 is the id, 1 is the blob, 2 and 3 are dates, 4 and 5 are the indexed fields for index2
        }
        assertEquals("docussandra", two.getKeyspace());
        assertEquals("INSERT INTO mydb_mytable_myindexwithtwofields (bucket, id, object, created_at, updated_at, myindexedfield1, myindexedfield2) VALUES (?, ?, ?, ?, ?, ?, ?);", two.preparedStatement().getQueryString());
    }

    /**
     * Test of generateDocumentCreateIndexEntriesStatements method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateDocumentCreateIndexEntriesStatementsWithDataTypes() throws IndexParseException
    {
        System.out.println("generateDocumentCreateIndexEntriesStatementsWithDataTypes");
        Document entity = Fixtures.createTestDocument3();
        f.insertIndex(index3);
        List<BoundStatement> result = IndexMaintainerHelper.generateDocumentCreateIndexEntriesStatements(f.getSession(), entity, new SimpleIndexBucketLocatorImpl());
        assertEquals(result.size(), 1);//one for each of our indices
        BoundStatement one = result.get(0);
        assertNotNull(one);
        for (int i = 0; i < 12; i++)// 0 is the id, 1 is the blob, 2 and 3 are dates, 4 - 11 are the indexed fields
        {
            assertTrue(one.isSet(i));
        }
        //check the proper types were set
        assertNotNull(one.getString(0));
        assertNotNull(one.getUUID(1));
        assertNotNull(one.getBytes(2));
        assertNotNull(one.getDate(3));
        assertNotNull(one.getDate(4));
        assertNotNull(one.getUUID(5));
        assertNotNull(one.getString(6));
        assertNotNull(one.getInt(7));
        assertNotNull(one.getDouble(8));
        assertNotNull(one.getBytes(9));
        assertNotNull(one.getBool(10));
        assertNotNull(one.getDate(11));
        assertEquals("docussandra", one.getKeyspace());
        assertEquals("INSERT INTO mydb_mytable_myindexallfields (bucket, id,"
                + " object, created_at, updated_at, thisisauudid, thisisastring, "
                + "thisisanint, thisisadouble, thisisbase64, thisisaboolean, thisisadate)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", one.preparedStatement().getQueryString());
    }

    /**
     * Test of generateDocumentCreateIndexEntriesStatements method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateDocumentCreateIndexEntriesStatementsNoIndexField() throws IndexParseException
    {
        System.out.println("testGenerateDocumentCreateIndexEntriesStatementsNoIndexField");
        Document entity = Fixtures.createTestDocument2();
        entity.object("{}");//good luck indexing that!
        List<BoundStatement> result = IndexMaintainerHelper.generateDocumentCreateIndexEntriesStatements(f.getSession(), entity, new SimpleIndexBucketLocatorImpl());
        assertTrue(result.isEmpty());
    }

    /**
     * Test of generateDocumentCreateIndexEntriesStatements method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateDocumentCreateIndexEntriesStatementsBadIndexField()
    {
        System.out.println("testGenerateDocumentCreateIndexEntriesStatementsBadIndexField");
        Document entity = Fixtures.createTestDocument3();
        f.insertIndex(Fixtures.createTestIndexAllFieldTypes());
        entity.object("{\"thisisastring\":\"hello\", \"thisisanint\": \"five\", \"thisisadouble\":\"five point five five five\","
                + " \"thisisbase64\":\"nope!\", \"thisisaboolean\":\"blah!\","
                + " \"thisisadate\":\"day 0\", \"thisisauudid\":\"z\"}");//completely botched field types
        boolean expectedExceptionThrown = false;
        try
        {
            List<BoundStatement> result = IndexMaintainerHelper.generateDocumentCreateIndexEntriesStatements(f.getSession(), entity, new SimpleIndexBucketLocatorImpl());
        } catch (IndexParseException e)
        {
            expectedExceptionThrown = true;
        }
        assertTrue("Expected exception was not thrown.", expectedExceptionThrown);
        
    }

    /**
     * Test of generateDocumentUpdateIndexEntriesStatements method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateDocumentUpdateIndexEntriesStatements() throws IndexParseException
    {
        System.out.println("generateDocumentUpdateIndexEntriesStatements");
        Document entity = Fixtures.createTestDocument2();
        tableRepo.create(table);//create the table so we have a place to store the test data
        docRepo.doCreate(entity);//insert a document so we have something to reference
        List<BoundStatement> result = IndexMaintainerHelper.generateDocumentUpdateIndexEntriesStatements(f.getSession(), entity, new SimpleIndexBucketLocatorImpl());
        assertEquals(2, result.size());//one for each of our indices
        BoundStatement one = result.get(0);
        assertNotNull(one);
        for (int i = 0; i < 3; i++)
        {
            assertTrue(one.isSet(i));// 0 is the blob, 1 is the date, 2 is the UUID
        }
        assertEquals("docussandra", one.getKeyspace());
        assertEquals("UPDATE mydb_mytable_myindexwithonefield SET object = ?, updated_at = ? WHERE bucket = ? AND myindexedfield = ?;", one.preparedStatement().getQueryString());
        BoundStatement two = result.get(1);
        assertNotNull(two);
        for (int i = 0; i < 4; i++)
        {
            assertTrue(two.isSet(i));// 0 is the blob, 1 is the date, 2 and 3 are indexed fields 
        }
        assertEquals("docussandra", two.getKeyspace());
        assertEquals("UPDATE mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ? WHERE bucket = ? AND myindexedfield1 = ? AND myindexedfield2 = ?;", two.preparedStatement().getQueryString());
    }

    /**
     * Test of generateDocumentUpdateIndexEntriesStatements method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateDocumentUpdateIndexEntriesStatementsWithDataTypes() throws IndexParseException
    {
        System.out.println("generateDocumentUpdateIndexEntriesStatements");
        Document entity = Fixtures.createTestDocument3();
        tableRepo.create(table);//create the table so we have a place to store the test data
        f.insertIndex(index3);
        docRepo.doCreate(entity);//insert a document so we have something to reference
        List<BoundStatement> result = IndexMaintainerHelper.generateDocumentUpdateIndexEntriesStatements(f.getSession(), entity, new SimpleIndexBucketLocatorImpl());
        assertEquals(1, result.size());//one for each of our indices
        BoundStatement one = result.get(0);
        assertNotNull(one);
        for (int i = 0; i < 10; i++)
        {
            assertTrue(one.isSet(i));// 0 is the blob, 1 is the date, 2 is the UUID, 3-10 are the other indexes
        }
        //check the proper types were set
        assertNotNull(one.getBytes(0));
        assertNotNull(one.getDate(1));
        assertNotNull(one.getString(2));
        assertNotNull(one.getUUID(3));
        assertNotNull(one.getString(4));
        assertNotNull(one.getInt(5));
        assertNotNull(one.getDouble(6));
        assertNotNull(one.getBytes(7));
        assertNotNull(one.getBool(8));
        assertNotNull(one.getDate(9));
        assertEquals("docussandra", one.getKeyspace());
        assertEquals("UPDATE mydb_mytable_myindexallfields SET object = ?, updated_at = ? "
                + "WHERE bucket = ? AND thisisauudid = ? AND thisisastring = ? AND thisisanint = ?"
                + " AND thisisadouble = ? AND thisisbase64 = ? AND thisisaboolean = ? AND thisisadate = ?;", one.preparedStatement().getQueryString());
    }

    /**
     * Test of generateDocumentUpdateIndexEntriesStatements method, of class
     * IndexMaintainerHelper. This test includes functionality for when an
     * indexed field has changed.
     */
    @Test
    public void testGenerateDocumentUpdateIndexEntriesStatementsIndexChanged() throws IndexParseException
    {
        System.out.println("generateDocumentUpdateIndexEntriesStatementsIndexChanged");
        Document entity = Fixtures.createTestDocument2();
        tableRepo.create(table);//create the table so we have a place to store the test data
        docRepo.doCreate(entity);//insert a document so we have something to reference
        entity.object("{'greeting':'hello', 'myindexedfield': 'this is NOT my field', 'myindexedfield1':'my second field', 'myindexedfield2':'my third field'}");//change an indexed field
        List<BoundStatement> result = IndexMaintainerHelper.generateDocumentUpdateIndexEntriesStatements(f.getSession(), entity, new SimpleIndexBucketLocatorImpl());
        assertEquals(3, result.size());//one for the create, one for the delete, one for the second index

        //create
        BoundStatement one = result.get(0);
        assertNotNull(one);
        for (int i = 0; i < 5; i++)
        {
            assertTrue(one.isSet(i));// 0 is the id, 1 is the blob, 2 and 3 are dates, 3 is the single index field for index1
        }
        assertEquals("docussandra", one.getKeyspace());
        assertEquals("INSERT INTO mydb_mytable_myindexwithonefield (bucket, id, object, created_at, updated_at, myindexedfield) VALUES (?, ?, ?, ?, ?, ?);", one.preparedStatement().getQueryString());
        //delete
        BoundStatement two = result.get(1);
        assertNotNull(one);
        assertTrue(two.isSet(0));//the UUID
        assertEquals("docussandra", two.getKeyspace());
        assertEquals("DELETE FROM mydb_mytable_myindexwithonefield WHERE bucket = ? AND myindexedfield = ?;", two.preparedStatement().getQueryString());

        //the index update should proceed like a normal update
        BoundStatement three = result.get(2);
        assertNotNull(three);
        for (int i = 0; i < 4; i++)
        {
            assertTrue(three.isSet(i));// 0 is the blob, 1 is the date, 2 and 3 are indexed fields 
        }
        assertEquals("docussandra", three.getKeyspace());
        assertEquals("UPDATE mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ? WHERE bucket = ? AND myindexedfield1 = ? AND myindexedfield2 = ?;", three.preparedStatement().getQueryString());
    }

    /**
     * Test of generateDocumentDeleteIndexEntriesStatements method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateDocumentDeleteIndexEntriesStatements() throws IndexParseException
    {
        System.out.println("generateDocumentDeleteIndexEntriesStatements");
        Document entity = Fixtures.createTestDocument2();
        entity.object("{'greeting':'hello', 'myindexedfield': 'this is my field', 'myindexedfield1':'my second field', 'myindexedfield2':'my third field'}");
        List<BoundStatement> result = IndexMaintainerHelper.generateDocumentDeleteIndexEntriesStatements(f.getSession(), entity, new SimpleIndexBucketLocatorImpl());
        assertEquals(2, result.size());//one for each of our indices (defined in the class setup methods)
        BoundStatement one = result.get(0);
        assertNotNull(one);
        assertTrue(one.isSet(0));//the UUID
        assertEquals("docussandra", one.getKeyspace());
        assertEquals("DELETE FROM mydb_mytable_myindexwithonefield WHERE bucket = ? AND myindexedfield = ?;", one.preparedStatement().getQueryString());
        BoundStatement two = result.get(1);
        assertNotNull(two);
        for (int i = 0; i < 1; i++)
        {
            assertTrue(two.isSet(i));// 0 and 1 are indexed fields 
        }
        assertEquals("docussandra", two.getKeyspace());
        assertEquals("DELETE FROM mydb_mytable_myindexwithtwofields WHERE bucket = ? AND myindexedfield1 = ? AND myindexedfield2 = ?;", two.preparedStatement().getQueryString());
    }

    /**
     * Test of generateDocumentDeleteIndexEntriesStatements method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateDocumentDeleteIndexEntriesStatementsWithDataTypes() throws IndexParseException
    {
        System.out.println("generateDocumentDeleteIndexEntriesStatementsWithDataTypes");
        Document entity = Fixtures.createTestDocument3();
        f.insertIndex(index3);
        entity.object("{\"thisisastring\":\"hello\", \"thisisanint\": \"5\", \"thisisadouble\":\"5.555\","
                + " \"thisisbase64\":\"VGhpcyBpcyBhIGdvb2RseSB0ZXN0IG1lc3NhZ2Uu\", \"thisisaboolean\":\"f\","
                + " \"thisisadate\":\"Thu Apr 30 09:52:04 MDT 2015\", \"thisisauudid\":\"3d069a5a-ef51-11e4-90ec-1681e6b88ec1\"}");
        List<BoundStatement> result = IndexMaintainerHelper.generateDocumentDeleteIndexEntriesStatements(f.getSession(), entity, new SimpleIndexBucketLocatorImpl());
        assertEquals(1, result.size());//one for each of our indices (defined in the class setup methods)
        BoundStatement one = result.get(0);
        assertNotNull(one);
        assertTrue(one.isSet(0));//the UUID
        assertNotNull(one.getString(0));
        assertNotNull(one.getUUID(1));
        assertNotNull(one.getString(2));
        assertNotNull(one.getInt(3));
        assertNotNull(one.getDouble(4));
        assertNotNull(one.getBytes(5));
        assertNotNull(one.getBool(6));
        assertNotNull(one.getDate(7));
        assertEquals("docussandra", one.getKeyspace());
        assertEquals("DELETE FROM mydb_mytable_myindexallfields WHERE bucket = ? AND thisisauudid = ? AND thisisastring = ? AND"
                + " thisisanint = ? AND thisisadouble = ? AND thisisbase64 = ? AND thisisaboolean = ? AND thisisadate = ?;",
                one.preparedStatement().getQueryString());

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
        assertEquals(2, result.size());
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
        String expResult = "INSERT INTO mydb_mytable_myindexwithonefield (bucket, id, object, created_at, updated_at, myindexedfield) VALUES (?, ?, ?, ?, ?, ?);";
        String result = IndexMaintainerHelper.generateCQLStatementForInsert(index1);
        assertEquals(expResult, result);
        expResult = "INSERT INTO mydb_mytable_myindexwithtwofields (bucket, id, object, created_at, updated_at, myindexedfield1, myindexedfield2) VALUES (?, ?, ?, ?, ?, ?, ?);";
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
        String expResult = "INSERT INTO mydb_mytable_myindexwithonefield (bucket, id, object, created_at, updated_at, myindexedfield) VALUES (?, ?, ?, ?, ?, ?);";
        String result = IndexMaintainerHelper.getCQLStatementForInsert(index1);
        assertEquals(expResult, result);
        expResult = "INSERT INTO mydb_mytable_myindexwithtwofields (bucket, id, object, created_at, updated_at, myindexedfield1, myindexedfield2) VALUES (?, ?, ?, ?, ?, ?, ?);";
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
        String expResult = "UPDATE mydb_mytable_myindexwithonefield SET object = ?, updated_at = ? WHERE bucket = ? AND myindexedfield = ?;";
        String result = IndexMaintainerHelper.generateCQLStatementForWhereClauses(IndexMaintainerHelper.ITABLE_UPDATE_CQL, index1);
        assertEquals(expResult, result);
        expResult = "UPDATE mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ? WHERE bucket = ? AND myindexedfield1 = ? AND myindexedfield2 = ?;";
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
        String expResult = "UPDATE mydb_mytable_myindexwithonefield SET object = ?, updated_at = ? WHERE bucket = ? AND myindexedfield = ?;";
        String result = IndexMaintainerHelper.getCQLStatementForWhereClauses(IndexMaintainerHelper.ITABLE_UPDATE_CQL, index1);
        assertEquals(expResult, result);
        expResult = "UPDATE mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ? WHERE bucket = ? AND myindexedfield1 = ? AND myindexedfield2 = ?;";
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

}

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
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.strategicgains.docussandra.bucketmanagement.SimpleIndexBucketLocatorImpl;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.persistence.DocumentRepository;
import com.strategicgains.docussandra.persistence.IndexChangeObserver;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.TableRepository;
import com.strategicgains.docussandra.testhelper.Fixtures;
import com.strategicgains.repoexpress.domain.Identifier;
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
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author udeyoje
 */
public class IndexMaintainerHelperTest {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private Session session;
    private IndexRepository indexRepo;
    private DocumentRepository docRepo;
    private TableRepository tableRepo;
    //some test records
    private Index index1 = createTestIndexOneField();
    private Index index2 = createTestIndexTwoField();
    private Table table;

    public IndexMaintainerHelperTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        Fixtures f = Fixtures.getInstance();
        Cluster cluster = Cluster.builder().addContactPoints(f.getCassandraSeeds()).build();
        final Metadata metadata = cluster.getMetadata();
        session = cluster.connect(f.getCassandraKeyspace());
        IndexChangeObserver ico = new IndexChangeObserver(session);
        indexRepo = new IndexRepository(session);
        docRepo = new DocumentRepository(session);
        tableRepo = new TableRepository(session);
        table = new Table();
        table.database(index1.databaseName().toLowerCase());
        table.name(index1.tableName().toLowerCase());
        table.setCreatedAt(new Date());
        logger.info("Connected to cluster: " + metadata.getClusterName() + '\n');
        clearTestData();// clear anything that might be there already
        //reinsert with some fresh data
        index1 = createTestIndexOneField();
        index2 = createTestIndexTwoField();
        indexRepo.doCreate(index1);
        ico.afterCreate(index1);//not sure why i have to call this explicitly
        indexRepo.doCreate(index2);
        ico.afterCreate(index2);

    }

    private void clearTestData() {
        try {
            indexRepo.delete(index1);
        } catch (InvalidQueryException e) {
            logger.debug("Not deleting index, probably doesn't exist.");
        }
        try {
            indexRepo.delete(index2);
        } catch (InvalidQueryException e) {
            logger.debug("Not deleting index, probably doesn't exist.");
        }
        try {
            docRepo.doDelete(createTestDocument());
        } catch (InvalidQueryException e) {
            logger.debug("Not deleting test document, probably doesn't exist.");
        }
        try {
            tableRepo.delete(table);
        } catch (InvalidQueryException e) {
            logger.debug("Not deleting test document, probably doesn't exist.");
        }
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of generateDocumentCreateIndexEntriesStatements method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateDocumentCreateIndexEntriesStatements() {
        System.out.println("generateDocumentCreateIndexEntriesStatements");
        Document entity = createTestDocument();
        List<BoundStatement> result = IndexMaintainerHelper.generateDocumentCreateIndexEntriesStatements(session, entity, new SimpleIndexBucketLocatorImpl());
        assertTrue(result.size() == 2);//one for each of our indices
        BoundStatement one = result.get(0);
        assertNotNull(one);
        for (int i = 0; i < 5; i++) {
            assertTrue(one.isSet(i));// 0 is the id, 1 is the blob, 2 and 3 are dates, 3 is the single index field for index1
        }
        assertEquals("docussandra", one.getKeyspace());
        assertEquals("INSERT INTO docussandra.mydb_mytable_myindexwithonefield (bucket, id, object, created_at, updated_at, myIndexedField) VALUES (?, ?, ?, ?, ?, ?);", one.preparedStatement().getQueryString());
        BoundStatement two = result.get(1);
        assertNotNull(two);
        for (int i = 0; i < 6; i++) {
            assertTrue(two.isSet(i));// 0 is the id, 1 is the blob, 2 and 3 are dates, 4 and 5 are the indexed fields for index2
        }
        assertEquals("docussandra", two.getKeyspace());
        assertEquals("INSERT INTO docussandra.mydb_mytable_myindexwithtwofields (bucket, id, object, created_at, updated_at, myIndexedField1,myIndexedField2) VALUES (?, ?, ?, ?, ?, ?, ?);", two.preparedStatement().getQueryString());
    }

    /**
     * Test of generateDocumentUpdateIndexEntriesStatements method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateDocumentUpdateIndexEntriesStatements() {
        System.out.println("generateDocumentUpdateIndexEntriesStatements");
        Document entity = createTestDocument();
        tableRepo.create(table);//create the table so we have a place to store the test data
        docRepo.doCreate(entity);//insert a document so we have something to reference
        List<BoundStatement> result = IndexMaintainerHelper.generateDocumentUpdateIndexEntriesStatements(session, entity, new SimpleIndexBucketLocatorImpl());
        assertTrue(result.size() == 2);//one for each of our indices
        BoundStatement one = result.get(0);
        assertNotNull(one);
        for (int i = 0; i < 3; i++) {
            assertTrue(one.isSet(i));// 0 is the blob, 1 is the date, 2 is the UUID
        }
        assertEquals("docussandra", one.getKeyspace());
        assertEquals("UPDATE docussandra.mydb_mytable_myindexwithonefield SET object = ?, updated_at = ? WHERE bucket = ? AND myIndexedField = ?;", one.preparedStatement().getQueryString());
        BoundStatement two = result.get(1);
        assertNotNull(two);
        for (int i = 0; i < 4; i++) {
            assertTrue(two.isSet(i));// 0 is the blob, 1 is the date, 2 and 3 are indexed fields 
        }
        assertEquals("docussandra", two.getKeyspace());
        assertEquals("UPDATE docussandra.mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ? WHERE bucket = ? AND myIndexedField1 = ? AND myIndexedField2 = ?;", two.preparedStatement().getQueryString());
    }

    /**
     * Test of generateDocumentUpdateIndexEntriesStatements method, of class
     * IndexMaintainerHelper. This test includes functionality for when an
     * indexed field has changed.
     */
    @Test
    public void testGenerateDocumentUpdateIndexEntriesStatementsIndexChanged() {
        System.out.println("generateDocumentUpdateIndexEntriesStatementsIndexChanged");
        Document entity = createTestDocument();
        tableRepo.create(table);//create the table so we have a place to store the test data
        docRepo.doCreate(entity);//insert a document so we have something to reference
        entity.object("{'greeting':'hello', 'myIndexedField': 'this is NOT my field', 'myIndexedField1':'my second field', 'myIndexedField2':'my third field'}");//change an indexed field
        List<BoundStatement> result = IndexMaintainerHelper.generateDocumentUpdateIndexEntriesStatements(session, entity, new SimpleIndexBucketLocatorImpl());
        assertTrue(result.size() == 3);//one for the create, one for the delete, one for the second index

        //create
        BoundStatement one = result.get(0);
        assertNotNull(one);
        for (int i = 0; i < 5; i++) {
            assertTrue(one.isSet(i));// 0 is the id, 1 is the blob, 2 and 3 are dates, 3 is the single index field for index1
        }
        assertEquals("docussandra", one.getKeyspace());
        assertEquals("INSERT INTO docussandra.mydb_mytable_myindexwithonefield (bucket, id, object, created_at, updated_at, myIndexedField) VALUES (?, ?, ?, ?, ?, ?);", one.preparedStatement().getQueryString());
        //delete
        BoundStatement two = result.get(1);
        assertNotNull(one);
        assertTrue(two.isSet(0));//the UUID
        assertEquals("docussandra", two.getKeyspace());
        assertEquals("DELETE FROM docussandra.mydb_mytable_myindexwithonefield WHERE bucket = ? AND myIndexedField = ?;", two.preparedStatement().getQueryString());

        //the index update should proceed like a normal update
        BoundStatement three = result.get(2);
        assertNotNull(three);
        for (int i = 0; i < 4; i++) {
            assertTrue(three.isSet(i));// 0 is the blob, 1 is the date, 2 and 3 are indexed fields 
        }
        assertEquals("docussandra", three.getKeyspace());
        assertEquals("UPDATE docussandra.mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ? WHERE bucket = ? AND myIndexedField1 = ? AND myIndexedField2 = ?;", three.preparedStatement().getQueryString());
    }

    /**
     * Test of generateDocumentDeleteIndexEntriesStatements method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateDocumentDeleteIndexEntriesStatements() {
        System.out.println("generateDocumentDeleteIndexEntriesStatements");
        Document entity = createTestDocument();
        entity.object("{'greeting':'hello', 'myIndexedField': 'this is my field', 'myIndexedField1':'my second field', 'myIndexedField2':'my third field'}");
        List<BoundStatement> result = IndexMaintainerHelper.generateDocumentDeleteIndexEntriesStatements(session, entity, new SimpleIndexBucketLocatorImpl());
        assertTrue(result.size() == 2);//one for each of our indices
        BoundStatement one = result.get(0);
        assertNotNull(one);
        assertTrue(one.isSet(0));//the UUID
        assertEquals("docussandra", one.getKeyspace());
        assertEquals("DELETE FROM docussandra.mydb_mytable_myindexwithonefield WHERE bucket = ? AND myIndexedField = ?;", one.preparedStatement().getQueryString());
        BoundStatement two = result.get(1);
        assertNotNull(two);
        for (int i = 0; i < 1; i++) {
            assertTrue(two.isSet(i));// 0 and 1 are indexed fields 
        }
        assertEquals("docussandra", two.getKeyspace());
        assertEquals("DELETE FROM docussandra.mydb_mytable_myindexwithtwofields WHERE bucket = ? AND myIndexedField1 = ? AND myIndexedField2 = ?;", two.preparedStatement().getQueryString());
    }

    /**
     * Test of populateNewIndexWithExistingData method, of class
     * IndexMaintainerHelper.
     */
    @Ignore
    @Test
    public void testPopulateNewIndexWithExistingData() {
        System.out.println("reindex");
        Session session = null;
        Table t = null;
        Index index = null;
        IndexMaintainerHelper.populateNewIndexWithExistingData(session, t, index);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getIndexForDocument method, of class IndexMaintainerHelper.
     */
    @Test
    public void testGetIndexForDocument() {
        System.out.println("getIndexForDocument");
        Document entity = createTestDocument();
        ArrayList<Index> exp = new ArrayList<>(2);
        exp.add(index1);
        exp.add(index2);
        List<Index> result = IndexMaintainerHelper.getIndexForDocument(session, entity);
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
    public void testGenerateCQLStatementForInsert() {
        System.out.println("generateCQLStatementForInsert");
        String expResult = "INSERT INTO docussandra.mydb_mytable_myindexwithonefield (bucket, id, object, created_at, updated_at, myIndexedField) VALUES (?, ?, ?, ?, ?, ?);";
        String result = IndexMaintainerHelper.generateCQLStatementForInsert(index1);
        assertEquals(expResult, result);
        expResult = "INSERT INTO docussandra.mydb_mytable_myindexwithtwofields (bucket, id, object, created_at, updated_at, myIndexedField1,myIndexedField2) VALUES (?, ?, ?, ?, ?, ?, ?);";
        result = IndexMaintainerHelper.generateCQLStatementForInsert(index2);
        assertEquals(expResult, result);
    }

    /**
     * Test of generateCQLStatementForWhereClauses method, of class
     * IndexMaintainerHelper.
     */
    @Test
    public void testGenerateCQLStatementForUpdate() {
        System.out.println("generateCQLStatementForUpdate");
        String expResult = "UPDATE docussandra.mydb_mytable_myindexwithonefield SET object = ?, updated_at = ? WHERE bucket = ? AND myIndexedField = ?;";
        String result = IndexMaintainerHelper.generateCQLStatementForWhereClauses(IndexMaintainerHelper.ITABLE_UPDATE_CQL, index1);
        assertEquals(expResult, result);
        expResult = "UPDATE docussandra.mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ? WHERE bucket = ? AND myIndexedField1 = ? AND myIndexedField2 = ?;";
        result = IndexMaintainerHelper.generateCQLStatementForWhereClauses(IndexMaintainerHelper.ITABLE_UPDATE_CQL, index2);
        assertEquals(expResult, result);
    }

    /**
     * Test of hasIndexedFieldChanged method, of class IndexMaintainerHelper.
     */
    @Test
    public void testHasIndexedFieldChanged() {
        System.out.println("hasIndexedFieldChanged");
        tableRepo.create(table);//create the table so we have a place to store the test data
        Document entity = createTestDocument();
        docRepo.doCreate(entity);//insert
        entity.object("{'greeting':'hola', 'myIndexedField': 'this is my field', 'myIndexedField1':'my second field', 'myIndexedField2':'my third field'}");//change a non-index field        
        boolean result = IndexMaintainerHelper.hasIndexedFieldChanged(session, index1, entity);
        assertEquals(false, result);
        entity.object("{'greeting':'hello', 'myIndexedField': 'this is NOT my field', 'myIndexedField1':'my second field', 'myIndexedField2':'my third field'}");//change an indexed field
        result = IndexMaintainerHelper.hasIndexedFieldChanged(session, index1, entity);
        assertEquals(true, result);
    }

    /**
     * Creates at test index with one field.
     *
     * @return
     */
    private static Index createTestIndexOneField() {
        Index index = new Index("myIndexWithOneField");
        index.table("myDB", "myTable");
        ArrayList<String> fields = new ArrayList<>();
        fields.add("myIndexedField");
        index.fields(fields);
        index.isUnique(false);
        return index;
    }

    /**
     * Creates at test index with two fields.
     *
     * @return
     */
    private static Index createTestIndexTwoField() {
        Index index = new Index("myIndexWithTwoFields");
        index.table("myDB", "myTable");
        ArrayList<String> fields = new ArrayList<>();
        fields.add("myIndexedField1");
        fields.add("myIndexedField2");
        index.fields(fields);
        index.isUnique(true);
        return index;
    }

    /**
     * Creates a test document.
     *
     * @return
     */
    //TODO: move to a TestHelper class
    public static final Document createTestDocument() {
        Document entity = new Document();
        entity.table("myDB", "myTable");
        entity.object("{'greeting':'hello', 'myIndexedField': 'this is my field', 'myIndexedField1':'my second field', 'myIndexedField2':'my third field'}");
        entity.setUuid(new UUID(0l, 1l));
        entity.setCreatedAt(new Date());
        entity.setUpdatedAt(new Date());
        return entity;
    }

}

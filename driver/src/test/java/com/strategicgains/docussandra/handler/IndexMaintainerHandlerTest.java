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
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.persistence.IndexChangeObserver;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.util.ArrayList;
import java.util.List;
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
public class IndexMaintainerHandlerTest {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private Session session;
    private IndexRepository indexRepo;
    //some test records
    private Index index1 = createTestIndexOneField();
    private Index index2 = createTestIndexTwoField();

    public IndexMaintainerHandlerTest() {
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
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of generateDocumentCreateIndexEntriesStatements method, of class
     * IndexMaintainerHandler.
     */
    @Test
    public void testGenerateDocumentCreateIndexEntriesStatements() {
        System.out.println("generateDocumentCreateIndexEntriesStatements");
        Document entity = new Document();
        entity.table("myDB", "myTable");
        entity.object("{'greeting':'hello', 'myIndexedField': 'this is my field', 'myIndexedField1':'my second field', 'myIndexedField2':'my third field'}");
        List<BoundStatement> result = IndexMaintainerHandler.generateDocumentCreateIndexEntriesStatements(session, entity);
        assertTrue(result.size() == 2);//one for each of our indices
        BoundStatement one = result.get(0);
        assertNotNull(one);
        for (int i = 0; i < 5; i++) {
            assertTrue(one.isSet(i));// 0 is the id, 1 is the blob, 2 and 3 are dates, 3 is the single index field for index1
        }
        assertEquals("docussandra", one.getKeyspace());
        assertEquals("INSERT INTO docussandra.mydb_mytable_myindexwithonefield (id, object, created_at, updated_at, myIndexedField) VALUES (?, ?, ?, ?, ?);", one.preparedStatement().getQueryString());
        BoundStatement two = result.get(1);
        assertNotNull(two);
        for (int i = 0; i < 6; i++) {
            assertTrue(two.isSet(i));// 0 is the id, 1 is the blob, 2 and 3 are dates, 4 and 5 are the indexed fields for index2
        }
        assertEquals("docussandra", two.getKeyspace());
        assertEquals("INSERT INTO docussandra.mydb_mytable_myindexwithtwofields (id, object, created_at, updated_at, myIndexedField1,myIndexedField2) VALUES (?, ?, ?, ?, ?, ?);", two.preparedStatement().getQueryString());
    }

    /**
     * Test of generateDocumentUpdateIndexEntriesStatements method, of class
     * IndexMaintainerHandler.
     */
    @Ignore
    @Test
    public void testGenerateDocumentUpdateIndexEntriesStatements() {
        System.out.println("generateDocumentUpdateIndexEntriesStatements");
        Session session = null;
        Document entity = null;
        List<BoundStatement> expResult = null;
        List<BoundStatement> result = IndexMaintainerHandler.generateDocumentUpdateIndexEntriesStatements(session, entity);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of generateDocumentDeleteIndexEntriesStatements method, of class
     * IndexMaintainerHandler.
     */
    @Ignore
    @Test
    public void testGenerateDocumentDeleteIndexEntriesStatements() {
        System.out.println("generateDocumentDeleteIndexEntriesStatements");
        Document entity = null;
        List<BoundStatement> expResult = null;
        List<BoundStatement> result = IndexMaintainerHandler.generateDocumentDeleteIndexEntriesStatements(session, entity);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of reindex method, of class IndexMaintainerHandler.
     */
    @Ignore
    @Test
    public void testReindex() {
        System.out.println("reindex");
        Session session = null;
        Table t = null;
        Index index = null;
        IndexMaintainerHandler.reindex(session, t, index);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getIndexForDocument method, of class IndexMaintainerHandler.
     */
    @Test
    public void testGetIndexForDocument() {
        System.out.println("getIndexForDocument");
        Document entity = new Document();
        entity.table("myDB", "myTable");
        ArrayList<Index> exp = new ArrayList<>(2);
        exp.add(index1);
        exp.add(index2);
        List<Index> result = IndexMaintainerHandler.getIndexForDocument(session, entity);
        assertNotNull(result);
        assertTrue(!result.isEmpty());
        assertTrue(result.size() == 2);
        assertEquals(exp, result);
    }

    /**
     * Test of generateCQLStatementForInsert method, of class
     * IndexMaintainerHandler.
     */
    @Test
    public void testGenerateCQLStatementForInsert() {
        System.out.println("generateCQLStatementForInsert");
        String expResult = "INSERT INTO docussandra.mydb_mytable_myindexwithonefield (id, object, created_at, updated_at, myIndexedField) VALUES (?, ?, ?, ?, ?);";
        String result = IndexMaintainerHandler.generateCQLStatementForInsert(index1);
        assertEquals(expResult, result);
        expResult = "INSERT INTO docussandra.mydb_mytable_myindexwithtwofields (id, object, created_at, updated_at, myIndexedField1,myIndexedField2) VALUES (?, ?, ?, ?, ?, ?);";
        result = IndexMaintainerHandler.generateCQLStatementForInsert(index2);
        assertEquals(expResult, result);
    }

    /**
     * Test of generateCQLStatementForUpdate method, of class
     * IndexMaintainerHandler.
     */
    @Test
    public void testGenerateCQLStatementForUpdate() {
        System.out.println("generateCQLStatementForUpdate");
        String expResult = "UPDATE docussandra.mydb_mytable_myindexwithonefield SET object = ?, updated_at = ?) WHERE id = ?;";
        String result = IndexMaintainerHandler.generateCQLStatementForUpdate(index1);
        assertEquals(expResult, result);
        expResult = "UPDATE docussandra.mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ?) WHERE myIndexedField1 = ? AND myIndexedField2 = ?;";
        result = IndexMaintainerHandler.generateCQLStatementForUpdate(index2);
        assertEquals(expResult, result);
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

}

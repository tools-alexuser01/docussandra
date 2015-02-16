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
import com.pearson.grid.pearsonlibrary.common.ReflectionUtil;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
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
        indexRepo = new IndexRepository(session);
        logger.info("Connected to cluster: " + metadata.getClusterName() + '\n');
        clearTestData();// clear anything that might be there already
        //reinsert with some fresh data
        index1 = createTestIndexOneField();
        index2 = createTestIndexTwoField();

        indexRepo.doCreate(index1);
        indexRepo.doCreate(index2);

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
    @Ignore
    @Test
    public void testGenerateDocumentCreateIndexEntriesStatements() {
        System.out.println("generateDocumentCreateIndexEntriesStatements");
        Session session = null;
        Document entity = null;
        List<BoundStatement> expResult = null;
        List<BoundStatement> result = IndexMaintainerHandler.generateDocumentCreateIndexEntriesStatements(session, entity);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
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
        Session session = null;
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
        //TODO: investigate: is this a bug? the result isn't returning any fields
        //assertEquals(exp, result);
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
        return index;
    }

}

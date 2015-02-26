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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.net.MediaType;
import com.mongodb.util.JSON;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.ParsedQuery;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.domain.WhereClause;
import com.strategicgains.docussandra.handler.IndexMaintainerHelperTest;
import static com.strategicgains.docussandra.persistence.ITableDaoTest.createTestIndexOneField;
import static com.strategicgains.docussandra.persistence.ITableDaoTest.createTestIndexTwoField;
import com.strategicgains.docussandra.testhelper.Fixtures;
import com.strategicgains.repoexpress.domain.Identifier;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.bson.BSONObject;
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
public class QueryDaoTest {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private Session session;

    public QueryDaoTest() {
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
        logger.info("Connected to cluster: " + metadata.getClusterName() + '\n');
        clearTestTables();
        createTestTables();
    }

    private void clearTestTables() {
        ITableDao cleanUpInstance = new ITableDao(session);
        try {
            cleanUpInstance.deleteITable("mydb_mytable_myindexwithonefield");
        } catch (InvalidQueryException e) {
            logger.debug("Not dropping iTable, probably doesn't exist.");
        }
        try {
            cleanUpInstance.deleteITable("mydb_mytable_myindexwithtwofields");
        } catch (InvalidQueryException e) {
            logger.debug("Not dropping iTable, probably doesn't exist.");
        }
        try {
            DocumentRepository docRepo = new DocumentRepository(session);
            docRepo.delete(this.createTestDocument());
        } catch (InvalidQueryException e) {
            logger.debug("Not dropping document, probably doesn't exist.");
        }
        try {
            TableRepository tableRepo = new TableRepository(session);
            tableRepo.deleteEntity(createTestTable());
        } catch (InvalidQueryException e) {
            logger.debug("Not dropping table, probably doesn't exist.");
        }
    }

    private void createTestTables() {
        System.out.println("createTestITables");
        Index index = ITableDaoTest.createTestIndexOneField();
        ITableDao instance = new ITableDao(session);
        instance.createITable(index);
        Index index2 = ITableDaoTest.createTestIndexTwoField();
        instance.createITable(index2);
        TableRepository tableRepo = new TableRepository(session);
        tableRepo.createEntity(createTestTable());
    }

    @After
    public void tearDown() {
        clearTestTables();
    }

    /**
     * Test of doQuery method, of class QueryDao.
     */
    @Test
    public void testDoQueryNoResults() {
        System.out.println("testDoQueryNoResults");
        QueryDao instance = new QueryDao(session);
        List<Document> result = instance.doQuery(ITableDaoTest.DB, createTestParsedQuery());
        assertNotNull(result);
        assertTrue(result.isEmpty());//no data yet, should get an empty set
    }

    /**
     * Test of doQuery method, of class QueryDao.
     */
    @Test
    public void testDoQueryWithResults() {
        System.out.println("testDoQueryWithResults");
        Document doc = this.createTestDocument();
        //put a test doc in
        DocumentRepository docRepo = new DocumentRepository(session);
        docRepo.doCreate(doc);
        QueryDao instance = new QueryDao(session);
        List<Document> result = instance.doQuery(ITableDaoTest.DB, createTestParsedQuery());
        assertNotNull(result);
        assertTrue(!result.isEmpty());
        assertTrue(result.size() == 1);
        Document res = result.get(0);
        assertNotNull(res);
        assertNotNull(res.getCreatedAt());
        assertNotNull(res.getUpdatedAt());
        assertNotNull(res.getUuid());
        assertNotNull(res.getId());
        assertNotNull(res.object());
        BSONObject expected = (BSONObject) JSON.parse(doc.object());
        BSONObject actual = (BSONObject) JSON.parse(res.object());
        assertEquals(expected, actual);
    }

    /**
     * Test of doQuery method, of class QueryDao.
     */
    @Test
    public void testDoQueryWithDataButNoResults() {
        System.out.println("testDoQueryWithDataButNoResults");
        Document doc = this.createTestDocument();
        //put a test doc in
        DocumentRepository docRepo = new DocumentRepository(session);
        docRepo.doCreate(doc);
        QueryDao instance = new QueryDao(session);
        List<Document> result = instance.doQuery(ITableDaoTest.DB, createTestParsedQuery2());
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    /**
     * Creates a simple parsed query based on a single index for testing.
     *
     * @return
     */
    //TODO: move to a TestHelper class
    public static final ParsedQuery createTestParsedQuery() {
        Query query = createTestQuery();
        WhereClause whereClause = new WhereClause(query.getWhere());
        String iTable = "mydb_mytable_myindexwithonefield";
        return new ParsedQuery(query, whereClause, iTable);
    }

    /**
     * Creates a simple query based on a single index for testing.
     *
     * @return
     */
    //TODO: move to a TestHelper class
    public static final Query createTestQuery() {
        Query query = new Query();
        query.setWhere("myindexedfield = 'thisismyfield'");
        query.setTable("mytable");
        return query;
    }

    /**
     * Creates a simple parsed query based on a single index for testing.
     *
     * @return
     */
    //TODO: move to a TestHelper class
    public static final ParsedQuery createTestParsedQuery2() {
        Query query = new Query();
        query.setWhere("myindexedfield = 'foo'");
        query.setTable("mytable");
        WhereClause whereClause = new WhereClause(query.getWhere());
        String iTable = "mydb_mytable_myindexwithonefield";
        return new ParsedQuery(query, whereClause, iTable);
    }

    /**
     * Creates a simple table for testing.
     *
     * @return
     */
    //TODO: move to a TestHelper class
    public static final Table createTestTable() {
        Table t = new Table();
        t.name("mytable");
        t.database(ITableDaoTest.DB);
        return t;
    }

    public static final Document createTestDocument() {
        Document entity = new Document();
        entity.table("myDB", "myTable");
        entity.object("{'greeting':'hello', 'myindexedfield': 'thisismyfield', 'myindexedfield1':'my second field', 'myindexedfield2':'my third field'}");
        entity.setUuid(new UUID(0l, 1l));
        entity.setCreatedAt(new Date());
        entity.setUpdatedAt(new Date());
        return entity;
    }
}

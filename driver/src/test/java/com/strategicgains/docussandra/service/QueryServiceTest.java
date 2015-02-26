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
package com.strategicgains.docussandra.service;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.mongodb.util.JSON;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.ParsedQuery;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.persistence.DocumentRepository;
import com.strategicgains.docussandra.persistence.ITableDao;
import com.strategicgains.docussandra.persistence.ITableDaoTest;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.QueryDao;
import com.strategicgains.docussandra.persistence.QueryDaoTest;
import com.strategicgains.docussandra.persistence.TableRepository;
import com.strategicgains.docussandra.testhelper.Fixtures;
import com.strategicgains.repoexpress.domain.Identifier;
import java.util.ArrayList;
import java.util.List;
import org.bson.BSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author udeyoje
 */
public class QueryServiceTest {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private Session session;
    private QueryService instance;

    public QueryServiceTest() {
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
        instance = new QueryService(new QueryDao(session));
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
            docRepo.delete(QueryDaoTest.createTestDocument());
        } catch (InvalidQueryException e) {
            logger.debug("Not dropping document, probably doesn't exist.");
        }
        try {
            TableRepository tableRepo = new TableRepository(session);
            tableRepo.delete(QueryDaoTest.createTestTable());
        } catch (InvalidQueryException e) {
            logger.debug("Not dropping table, probably doesn't exist.");
        }
        try {
            IndexRepository indexRepo = new IndexRepository(session);
            indexRepo.delete(ITableDaoTest.createTestIndexOneField());
        } catch (InvalidQueryException e) {
            logger.debug("Not dropping table, probably doesn't exist.");
        }
    }

    private void createTestTables() {
        System.out.println("createTestITables");
        Index index = ITableDaoTest.createTestIndexOneField();
        IndexRepository indexRepo = new IndexRepository(session);
        indexRepo.create(index);
        ITableDao instance = new ITableDao(session);
        //instance.createITable(index);
        Index index2 = ITableDaoTest.createTestIndexTwoField();
        instance.createITable(index2);
        TableRepository tableRepo = new TableRepository(session);
        tableRepo.create(QueryDaoTest.createTestTable());

    }

    @After
    public void tearDown() {
        clearTestTables();
    }

    /**
     * Test of query method, of class QueryService.
     */
    @Test
    public void testQuery() {
        System.out.println("query");
        Document doc = QueryDaoTest.createTestDocument();
        //put a test doc in
        DocumentRepository docRepo = new DocumentRepository(session);
        docRepo.doCreate(doc);
        List<Document> result = instance.query(ITableDaoTest.DB, QueryDaoTest.createTestQuery());
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
     * Test of parseQuery method, of class QueryService.
     */
    @Test
    @Ignore
    public void testParseQuery() {
        System.out.println("parseQuery");
        String db = ITableDaoTest.DB;
        Query toParse = null;
        ParsedQuery expResult = null;
        ParsedQuery result = instance.parseQuery(db, toParse);
        assertEquals(expResult, result);
    }

    /**
     * Test of equalLists method, of class QueryService.
     */
    @Test
    public void testEqualLists() {
        System.out.println("equalLists");
        List<String> one = new ArrayList<>();
        List<String> two = new ArrayList<>();
        boolean result = instance.equalLists(one, two);
        assertEquals(true, result);
        one.add("one");
        one.add("two");
        two.add("two");
        two.add("one");
        result = instance.equalLists(one, two);
        assertEquals(true, result);
        two.add("three");
        result = instance.equalLists(one, two);
        assertEquals(false, result);
    }

}

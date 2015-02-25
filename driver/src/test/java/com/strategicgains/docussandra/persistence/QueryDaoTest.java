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
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.ParsedQuery;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.domain.WhereClause;
import static com.strategicgains.docussandra.persistence.ITableDaoTest.createTestIndexOneField;
import static com.strategicgains.docussandra.persistence.ITableDaoTest.createTestIndexTwoField;
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
        clearTestITables();
        createTestITables();
    }

    private void clearTestITables() {
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
    }

    private void createTestITables() {
        System.out.println("createTestITables");
        Index index = ITableDaoTest.createTestIndexOneField();
        ITableDao instance = new ITableDao(session);
        instance.createITable(index);
        Index index2 = ITableDaoTest.createTestIndexTwoField();
        instance.createITable(index2);
    }

    @After
    public void tearDown() {
        clearTestITables();
    }

    /**
     * Test of doQuery method, of class QueryDao.
     */
    @Test
    public void testDoQueryNoResults() {
        System.out.println("doQuery");                
        QueryDao instance = new QueryDao(session);
        List<Document> result = instance.doQuery(ITableDaoTest.DB, createTestParsedQuery());
        assertNotNull(result);
        assertTrue(result.isEmpty());//no data yet, should get an empty set
    }
    
    /**
     * Creates a simple parsed query based on a single index for testing.
     * @return 
     */
    private ParsedQuery createTestParsedQuery(){
        Query query = new Query();
        query.setWhere("myIndexedField = 'foobar'");
        query.setTable("mytable");
        WhereClause whereClause = new WhereClause(query.getWhere());
        String iTable = "mydb_mytable_myindexwithonefield";
        return new ParsedQuery(query, whereClause, iTable);
    }

}

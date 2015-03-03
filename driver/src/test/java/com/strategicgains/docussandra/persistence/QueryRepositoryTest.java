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

import com.mongodb.util.JSON;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.util.List;
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
public class QueryRepositoryTest
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private Fixtures f;

    public QueryRepositoryTest()
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
    }

    @Before
    public void setUp()
    {
        f.clearTestTables();
        f.createTestTables();
    }

    @After
    public void tearDown()
    {
        f.clearTestTables();
    }

    /**
     * Test of doQuery method, of class QueryDao.
     */
    @Test
    public void testDoQueryNoResults()
    {
        System.out.println("testDoQueryNoResults");
        QueryRepository instance = new QueryRepository(f.getSession());
        List<Document> result = instance.doQuery(Fixtures.DB, Fixtures.createTestParsedQuery());
        assertNotNull(result);
        assertTrue(result.isEmpty());//no data yet, should get an empty set
    }

    /**
     * Test of doQuery method, of class QueryDao.
     */
    @Test
    public void testDoQueryWithResults()
    {
        System.out.println("testDoQueryWithResults");
        Document doc = Fixtures.createTestDocument();
        //put a test doc in
        DocumentRepository docRepo = new DocumentRepository(f.getSession());
        docRepo.doCreate(doc);
        QueryRepository instance = new QueryRepository(f.getSession());
        List<Document> result = instance.doQuery(Fixtures.DB, Fixtures.createTestParsedQuery());
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
    public void testDoQueryWithDataButNoResults()
    {
        System.out.println("testDoQueryWithDataButNoResults");
        Document doc = Fixtures.createTestDocument();
        //put a test doc in
        DocumentRepository docRepo = new DocumentRepository(f.getSession());
        docRepo.doCreate(doc);
        QueryRepository instance = new QueryRepository(f.getSession());
        List<Document> result = instance.doQuery(Fixtures.DB, Fixtures.createTestParsedQuery2());
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

//    /**
//     * Test of doQuery method, of class QueryDao.
//     */
//    @Test
//    public void testDoQueryWithLotsOfResults() throws Exception
//    {
//        System.out.println("testDoQueryWithLotsOfResults");
//        //put in an index that we can use with bulk data
//        f.insertIndex(Fixtures.createTestIndexWithBulkDataHit());
//        List<Document> docs = Fixtures.getBulkDocuments();
//        //put a bunch of test docs in
//        f.insertDocuments(docs);
//        QueryRepository instance = new QueryRepository(f.getSession());
//        List<Document> result = instance.doQuery(Fixtures.DB, Fixtures.createTestParsedQueryBulkData());
//        assertNotNull(result);
//        assertTrue(!result.isEmpty());
//        assertTrue(result.size() == 34);
//    }

}

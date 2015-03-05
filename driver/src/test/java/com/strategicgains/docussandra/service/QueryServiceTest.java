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

import com.mongodb.util.JSON;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.ParsedQuery;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.domain.WhereClause;
import com.strategicgains.docussandra.exception.FieldNotIndexedException;
import com.strategicgains.docussandra.persistence.DocumentRepository;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.QueryRepository;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.util.ArrayList;
import java.util.List;
import org.bson.BSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author udeyoje
 */
public class QueryServiceTest
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private QueryService instance;
    private Fixtures f;

    public QueryServiceTest()
    {
        f = Fixtures.getInstance();
    }

    @Before
    public void setUp()
    {
        f.clearTestTables();
        f.createTestTables();
        instance = new QueryService(new QueryRepository(f.getSession()));
        IndexRepository indexRepo = new IndexRepository(f.getSession());
        indexRepo.create(Fixtures.createTestIndexTwoField());
    }

    @After
    public void tearDown()
    {
        f.clearTestTables();
    }

    /**
     * Test of query method, of class QueryService.
     */
    @Test
    public void testQuery()
    {
        System.out.println("query");
        Document doc = Fixtures.createTestDocument();
        //put a test doc in
        DocumentRepository docRepo = new DocumentRepository(f.getSession());
        docRepo.doCreate(doc);
        List<Document> result = instance.query(Fixtures.DB, Fixtures.createTestQuery());
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
    public void testParseQueryBasic()
    {
        System.out.println("testParseQueryBasic");
        String db = Fixtures.DB;
        Query toParse = Fixtures.createTestQuery();
        ParsedQuery expResult = new ParsedQuery(toParse, new WhereClause(toParse.getWhere()), "mydb_mytable_myindexwithonefield");
        ParsedQuery result = instance.parseQuery(db, toParse);
        assertEquals(expResult, result);
    }

    /**
     * Test of parseQuery method, of class QueryService.
     */
    @Test
    public void testParseQueryTwoFields()
    {
        System.out.println("testParseQueryTwoFields");
        String db = Fixtures.DB;
        Query toParse = Fixtures.createTestQuery2();
        ParsedQuery expResult = new ParsedQuery(toParse, new WhereClause(toParse.getWhere()), "mydb_mytable_myindexwithtwofields");
        ParsedQuery result = instance.parseQuery(db, toParse);
        assertEquals(expResult, result);
    }

    /**
     * Test of parseQuery method, of class QueryService.
     */
    @Test
    public void testParseQueryTwoFieldsImperfectMatch()
    {
        System.out.println("testParseQueryTwoFieldsImperfectMatch");
        String db = Fixtures.DB;
        Query toParse = Fixtures.createTestQuery2();
        toParse.setWhere("myindexedfield1 = 'thisismyfield'");
        ParsedQuery expResult = new ParsedQuery(toParse, new WhereClause(toParse.getWhere()), "mydb_mytable_myindexwithtwofields");
        ParsedQuery result = instance.parseQuery(db, toParse);
        assertEquals(expResult, result);
    }

    /**
     * Test of parseQuery method, of class QueryService.
     */
    @Test
    public void testParseQueryException()
    {
        System.out.println("testParseQueryException");
        String db = Fixtures.DB;
        Query toParse = Fixtures.createTestQuery();
        toParse.setWhere("nonIndexedField = 'boo'");
        boolean expectedExceptionThrown = false;
        try
        {
            ParsedQuery result = instance.parseQuery(db, toParse);
        } catch (FieldNotIndexedException e)
        {
            expectedExceptionThrown = true;
            assertTrue(e.getLocalizedMessage().contains("nonIndexedField"));
        }
        assertTrue("Expected exception not thrown", expectedExceptionThrown);
    }

    /**
     * Test of equalLists method, of class QueryService.
     */
    @Test
    public void testEqualLists()
    {
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

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

import com.strategicgains.docussandra.cache.CacheFactory;
import com.strategicgains.docussandra.domain.ParsedQuery;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.domain.WhereClause;
import com.strategicgains.docussandra.exception.FieldNotIndexedException;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author udeyoje
 */
public class ParsedQueryFactoryTest
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static Fixtures f;

    public ParsedQueryFactoryTest() throws Exception
    {
        f = Fixtures.getInstance();
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
    }

    @AfterClass
    public static void tearDownClass() throws Exception
    {
        f.clearTestTables();
    }

    @Before
    public void setUp()
    {
        f.clearTestTables();
        f.createTestITables();
        IndexRepository indexRepo = new IndexRepository(f.getSession());
        indexRepo.create(Fixtures.createTestIndexTwoField());
    }

    @After
    public void tearDown()
    {
        
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
        ParsedQuery result = ParsedQueryFactory.parseQuery(db, toParse, f.getSession());
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
        ParsedQuery result = ParsedQueryFactory.parseQuery(db, toParse, f.getSession());
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
        ParsedQuery result = ParsedQueryFactory.parseQuery(db, toParse, f.getSession());
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
            ParsedQuery result = ParsedQueryFactory.parseQuery(db, toParse, f.getSession());
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
        boolean result = ParsedQueryFactory.equalLists(one, two);
        assertEquals(true, result);
        one.add("one");
        one.add("two");
        two.add("two");
        two.add("one");
        result = ParsedQueryFactory.equalLists(one, two);
        assertEquals(true, result);
        two.add("three");
        result = ParsedQueryFactory.equalLists(one, two);
        assertEquals(false, result);
    }

    /**
     * Test of getParsedQuery method, of class ParsedQueryFactory.
     */
    @Test
    public void testGetParsedQuery()
    {
        System.out.println("testGetParsedQuery");
        String db = Fixtures.DB;
        CacheFactory.clearAllCaches();//kill the cache and make it re-create for the purposes of this test.
        Query toParse = Fixtures.createTestQuery();
        ParsedQuery expResult = new ParsedQuery(toParse, new WhereClause(toParse.getWhere()), "mydb_mytable_myindexwithonefield");
        ParsedQuery result = ParsedQueryFactory.getParsedQuery(db, toParse, f.getSession());
        assertEquals(expResult, result);
        //try again to ensure the cache isn't botched
        result = ParsedQueryFactory.getParsedQuery(db, toParse, f.getSession());
        assertEquals(expResult, result);
    }

    /**
     * Test of getParsedQuery method, of class QueryService.
     */
    @Test
    public void testGetParsedQueryException()
    {
        System.out.println("testGetParsedQueryException");
        String db = Fixtures.DB;
        Query toParse = Fixtures.createTestQuery();
        toParse.setWhere("nonIndexedField = 'boo'");
        boolean expectedExceptionThrown = false;
        try
        {
            ParsedQuery result = ParsedQueryFactory.parseQuery(db, toParse, f.getSession());
        } catch (FieldNotIndexedException e)
        {
            expectedExceptionThrown = true;
            assertTrue(e.getLocalizedMessage().contains("nonIndexedField"));
        }
        assertTrue("Expected exception not thrown", expectedExceptionThrown);
    }

}

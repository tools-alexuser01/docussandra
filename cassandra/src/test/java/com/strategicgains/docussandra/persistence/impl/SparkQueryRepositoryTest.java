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
package com.strategicgains.docussandra.persistence.impl;

import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.domain.QueryResponseWrapper;
import com.strategicgains.docussandra.testhelper.Fixtures;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 *
 * @author udeyoje
 */
public class SparkQueryRepositoryTest
{
    
    public SparkQueryRepositoryTest()
    {
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
    }
    
    @After
    public void tearDown()
    {
    }

    /**
     * Test of query method, of class SparkQueryRepository.
     */
    @Test
    @Ignore
    public void testQueryCassandra()
    {
        System.out.println("query");
        Query query = Fixtures.createTestQuery();
        SparkQueryRepository instance = new SparkQueryRepository();
        QueryResponseWrapper expResult = null;
        QueryResponseWrapper result = instance.queryCassandra(query);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
        /**
     * Test of query method, of class SparkQueryRepository.
     */
    @Test
    @Ignore
    public void testQueryHadoop()
    {
        System.out.println("query");
        Query query = Fixtures.createTestQuery();
        SparkQueryRepository instance = new SparkQueryRepository();
        QueryResponseWrapper expResult = null;
        QueryResponseWrapper result = instance.queryHadoop(query);
    }
    
}

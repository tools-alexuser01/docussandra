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
package com.strategicgains.docussandra.persistence.helper;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.testhelper.Fixtures;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author udeyoje
 */
public class PreparedStatementFactoryTest
{
    private Fixtures f;
    
    public PreparedStatementFactoryTest()
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
    }
    
    @After
    public void tearDown()
    {
    }

    /**
     * Test of getPreparedStatement method, of class PreparedStatementFactory.
     */
    @Test
    public void testGetPreparedStatement()
    {
        System.out.println("getPreparedStatement");
        String query = "select * from docussandra.sys_db";
        PreparedStatement result = PreparedStatementFactory.getPreparedStatement(query, f.getSession());
        assertNotNull(result);
        result = PreparedStatementFactory.getPreparedStatement(query, f.getSession());
        assertNotNull(result);
    }
    
}

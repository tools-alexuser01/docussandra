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
package com.strategicgains.docussandra.domain;

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
public class WhereClauseTest
{

    public WhereClauseTest()
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

    @Test
    public void testIt0()
    {
        WhereClause wc = new WhereClause("blah = 'nonsense'");
        assertEquals("blah = ?", wc.getBoundStatementSyntax());
        assertEquals("blah", wc.getFields().get(0));
        assertEquals("nonsense", wc.getValues().get(0));
    }

    @Test
    public void testIt1()
    {
        WhereClause wc = new WhereClause("blah = 'blah' AND foo = 'bar'");
        assertEquals("blah = ? AND foo = ?", wc.getBoundStatementSyntax());
        assertEquals("blah", wc.getFields().get(0));
        assertEquals("foo", wc.getFields().get(1));
        assertEquals("blah", wc.getValues().get(0));
        assertEquals("bar", wc.getValues().get(1));
    }

    @Test
    public void testIt2()
    {
        WhereClause wc = new WhereClause("blah = 'blah' AND foo = 'bar' ORDER BY foo");
        assertEquals("blah = ? AND foo = ? ORDER BY foo", wc.getBoundStatementSyntax());
        assertEquals("blah", wc.getFields().get(0));
        assertEquals("foo", wc.getFields().get(1));
        assertEquals("blah", wc.getValues().get(0));
        assertEquals("bar", wc.getValues().get(1));
    }

    @Test
    public void testIt3()
    {
        WhereClause wc = new WhereClause("blah = 'blah' AND foo < 'bar' ORDER BY foo");
        assertEquals("blah = ? AND foo < ? ORDER BY foo", wc.getBoundStatementSyntax());
        assertEquals("blah", wc.getFields().get(0));
        assertEquals("foo", wc.getFields().get(1));
        assertEquals("blah", wc.getValues().get(0));
        assertEquals("bar", wc.getValues().get(1));
    }

    @Test
    public void testIt4()
    {
        WhereClause wc = new WhereClause("blah = 'non sense'");
        assertEquals("blah = ?", wc.getBoundStatementSyntax());
        assertEquals("blah", wc.getFields().get(0));
        assertEquals("non sense", wc.getValues().get(0));
    }

    @Test
    public void testIt5()
    {
        WhereClause wc = new WhereClause("blah = 'blah blah' AND foo = 'bar bar bar'");
        assertEquals("blah = ? AND foo = ?", wc.getBoundStatementSyntax());
        assertEquals("blah", wc.getFields().get(0));
        assertEquals("foo", wc.getFields().get(1));
        assertEquals("blah blah", wc.getValues().get(0));
        assertEquals("bar bar bar", wc.getValues().get(1));
    }
}

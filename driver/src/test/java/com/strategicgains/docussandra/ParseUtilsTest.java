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
package com.strategicgains.docussandra;

import com.strategicgains.docussandra.exception.IndexParseFieldException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.util.Date;
import java.util.TimeZone;
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
public class ParseUtilsTest
{

    private static Logger logger = LoggerFactory.getLogger(ParseUtilsTest.class);

    public ParseUtilsTest()
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
     * Test of parseBase64StringAsByteBuffer method, of class ParseUtils.
     */
    @Test
    public void testParseBase64StringAsByteBuffer() throws Exception
    {
        System.out.println("parseBase64StringAsByteBuffer");
        //good test
        String in = "VGhpcyBpcyBhIGdvb2RseSB0ZXN0IG1lc3NhZ2Uu";
        ByteBuffer result = ParseUtils.parseBase64StringAsByteBuffer(in);
        assertNotNull(result);
        assertTrue(result.hasArray());
        assertTrue(result.hasRemaining());
        assertTrue(result.array().length != 0);
        assertEquals(new String(result.array()), "This is a goodly test message.");
    }

    /**
     * Test of parseStringAsBoolean method, of class ParseUtils.
     */
    @Test
    public void testParseStringAsBoolean() throws Exception
    {
        System.out.println("parseStringAsBoolean");
        //false
        boolean result = ParseUtils.parseStringAsBoolean("false");
        assertEquals(false, result);
        result = ParseUtils.parseStringAsBoolean("False");
        assertEquals(false, result);
        result = ParseUtils.parseStringAsBoolean("faLSe");
        assertEquals(false, result);
        result = ParseUtils.parseStringAsBoolean("FALSE");
        assertEquals(false, result);
        result = ParseUtils.parseStringAsBoolean("f");
        assertEquals(false, result);
        result = ParseUtils.parseStringAsBoolean("F");
        assertEquals(false, result);
        result = ParseUtils.parseStringAsBoolean("0");
        assertEquals(false, result);

        //true
        result = ParseUtils.parseStringAsBoolean("true");
        assertEquals(true, result);
        result = ParseUtils.parseStringAsBoolean("True");
        assertEquals(true, result);
        result = ParseUtils.parseStringAsBoolean("tRUe");
        assertEquals(true, result);
        result = ParseUtils.parseStringAsBoolean("TRUE");
        assertEquals(true, result);
        result = ParseUtils.parseStringAsBoolean("t");
        assertEquals(true, result);
        result = ParseUtils.parseStringAsBoolean("T");
        assertEquals(true, result);
        result = ParseUtils.parseStringAsBoolean("1");
        assertEquals(true, result);

        //exception
        boolean expectExceptionThrown = false;
        try
        {
            ParseUtils.parseStringAsBoolean("");
        } catch (IndexParseFieldException e)
        {
            expectExceptionThrown = true;
            assertNull(e.getCause());
            assertNotNull(e.getMessage());
        }
        assertTrue(expectExceptionThrown);

        expectExceptionThrown = false;
        try
        {
            ParseUtils.parseStringAsBoolean("blah");
        } catch (IndexParseFieldException e)
        {
            expectExceptionThrown = true;
            assertNull(e.getCause());
            assertNotNull(e.getMessage());
            assertTrue(e.getMessage().contains("blah"));
            assertEquals(e.getFieldValue(), "blah");
        }
        assertTrue(expectExceptionThrown);
    }

    /**
     * Test of parseStringAsDate method, of class ParseUtils.
     */
    @Test
    public void testParseStringAsDate() throws Exception
    {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        System.out.println("parseStringAsDate");
        Date testDate = new Date();
        String in = testDate.toString();
        Date result = ParseUtils.parseStringAsDate(in);
        assertEquals(testDate.getTime(), result.getTime(), 100l);

        DateFormat format = DateFormat.getDateInstance(DateFormat.LONG);
        in = format.format(testDate);
        result = ParseUtils.parseStringAsDate(in);
        assertEquals(testDate.getTime(), result.getTime(), 3600l);

        testDate = new Date();
        testDate.setYear(3);
        testDate.setMonth(11);
        testDate.setDate(17);
        testDate.setHours(0);
        testDate.setMinutes(0);
        testDate.setSeconds(0);
        //testDate.setTime(testDate.getTime() + (3600000 * -8));
        in = "12/17/1903";
        result = ParseUtils.parseStringAsDate(in);
        assertEquals(testDate.getTime(), result.getTime(), 0l);

        //in = "17/12/1903";//nope!
        in = "17 Dec 1903 00:00:00";
        result = ParseUtils.parseStringAsDate(in);
        assertEquals(testDate.getTime(), result.getTime(), 0l);
    }

    /**
     * Test of parseStringAsDouble method, of class ParseUtils.
     */
    @Test
    public void testParseStringAsDouble() throws Exception
    {
        System.out.println("parseStringAsDouble");
        String in = "0";
        double expResult = 0.0;
        double result = ParseUtils.parseStringAsDouble(in);
        assertEquals(expResult, result, 0.0);
        in = "1.123456";
        expResult = 1.123456;
        result = ParseUtils.parseStringAsDouble(in);
        assertEquals(expResult, result, 0.0);
        in = "-1.123456";
        expResult = -1.123456;
        result = ParseUtils.parseStringAsDouble(in);
        assertEquals(expResult, result, 0.0);
        boolean expectExceptionThrown = false;
        try
        {
            ParseUtils.parseStringAsDouble("dafhfda");
        } catch (IndexParseFieldException e)
        {
            expectExceptionThrown = true;
            assertNotNull(e.getCause());
            assertNotNull(e.getMessage());
        }
        assertTrue(expectExceptionThrown);
    }

    /**
     * Test of parseStringAsInt method, of class ParseUtils.
     */
    @Test
    public void testParseStringAsInteger() throws Exception
    {
        System.out.println("parseStringAsInt");
        String in = "0";
        int expResult = 0;
        int result = ParseUtils.parseStringAsInt(in);
        assertEquals(expResult, result);
        in = "1";
        expResult = 1;
        result = ParseUtils.parseStringAsInt(in);
        assertEquals(expResult, result, 0.0);
        in = "-1";
        expResult = -1;
        result = ParseUtils.parseStringAsInt(in);
        assertEquals(expResult, result, 0.0);
        boolean expectExceptionThrown = false;
        try
        {
            ParseUtils.parseStringAsInt("dafhfda");
        } catch (IndexParseFieldException e)
        {
            expectExceptionThrown = true;
            assertNotNull(e.getCause());
            assertNotNull(e.getMessage());
        }
        assertTrue(expectExceptionThrown);
    }

}

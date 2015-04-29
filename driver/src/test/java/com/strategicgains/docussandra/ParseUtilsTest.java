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
     * Test of convertBase64StringToByteBuffer method, of class ParseUtils.
     */
    @Test
    public void testConvertBase64StringToByteBuffer() throws Exception
    {
        System.out.println("convertBase64StringToByteBuffer");
        //good test
        String in = "VGhpcyBpcyBhIGdvb2RseSB0ZXN0IG1lc3NhZ2Uu";
        ByteBuffer result = ParseUtils.convertBase64StringToByteBuffer(in);
        assertNotNull(result);
        assertTrue(result.hasArray());
        assertTrue(result.hasRemaining());
        assertTrue(result.array().length != 0);
        assertEquals(new String(result.array()), "This is a goodly test message.");
    }

    /**
     * Test of convertStringToBoolean method, of class ParseUtils.
     */
    @Test
    public void testConvertStringToBoolean() throws Exception
    {
        System.out.println("convertStringToBoolean");
        //false
        boolean result = ParseUtils.convertStringToBoolean("false");
        assertEquals(false, result);
        result = ParseUtils.convertStringToBoolean("False");
        assertEquals(false, result);
        result = ParseUtils.convertStringToBoolean("faLSe");
        assertEquals(false, result);
        result = ParseUtils.convertStringToBoolean("FALSE");
        assertEquals(false, result);
        result = ParseUtils.convertStringToBoolean("f");
        assertEquals(false, result);
        result = ParseUtils.convertStringToBoolean("F");
        assertEquals(false, result);
        result = ParseUtils.convertStringToBoolean("0");
        assertEquals(false, result);

        //true
        result = ParseUtils.convertStringToBoolean("true");
        assertEquals(true, result);
        result = ParseUtils.convertStringToBoolean("True");
        assertEquals(true, result);
        result = ParseUtils.convertStringToBoolean("tRUe");
        assertEquals(true, result);
        result = ParseUtils.convertStringToBoolean("TRUE");
        assertEquals(true, result);
        result = ParseUtils.convertStringToBoolean("t");
        assertEquals(true, result);
        result = ParseUtils.convertStringToBoolean("T");
        assertEquals(true, result);
        result = ParseUtils.convertStringToBoolean("1");
        assertEquals(true, result);

        //exception
        boolean expectExceptionThrown = false;
        try
        {
            ParseUtils.convertStringToBoolean("");
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
            ParseUtils.convertStringToBoolean("blah");
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
     * Test of convertStringToDate method, of class ParseUtils.
     */
    @Test
    public void testConvertStringToDate() throws Exception
    {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        System.out.println("convertStringToDate");
        Date testDate = new Date();
        String in = testDate.toString();
        Date result = ParseUtils.convertStringToDate(in);
        assertEquals(testDate.getTime(), result.getTime(), 100l);

        DateFormat format = DateFormat.getDateInstance(DateFormat.LONG);
        in = format.format(testDate);
        result = ParseUtils.convertStringToDate(in);
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
        result = ParseUtils.convertStringToDate(in);
        assertEquals(testDate.getTime(), result.getTime(), 0l);

        //in = "17/12/1903";//nope!
        in = "17 Dec 1903 00:00:00";
        result = ParseUtils.convertStringToDate(in);
        assertEquals(testDate.getTime(), result.getTime(), 0l);
    }

}

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
package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.testhelper.Fixtures;
import com.strategicgains.repoexpress.domain.Identifier;
import java.util.Date;
import java.util.UUID;
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
public class IndexCreatedEventTest
{

    public IndexCreatedEventTest()
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
     * Test of isDoneIndexing method, of class IndexCreatedEvent.
     */
    @Test
    public void testIsDoneIndexing()
    {
        System.out.println("isDoneIndexing");
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        boolean result = instance.isDoneIndexing();
        assertEquals(false, result);
        Index i = Fixtures.createTestIndexOneField();
        i.setActive(true);
        instance.setIndex(i);
        result = instance.isDoneIndexing();
        assertEquals(true, result);
    }

    /**
     * Test of getUuid method, of class IndexCreatedEvent.
     */
    @Test
    public void testGetUuid()
    {
        System.out.println("getUuid");
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        UUID result = instance.getUuid();
        assertEquals(instance.getUuid(), result);//this is obvious, but it might catch a NPE or something
    }

    /**
     * Test of setUuid method, of class IndexCreatedEvent.
     */
    @Test
    public void testSetUuid()
    {
        System.out.println("setUuid");
        UUID id = new UUID(0, 1);
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        instance.setUuid(id);
        assertEquals(id, instance.getUuid());
    }

    /**
     * Test of getId method, of class IndexCreatedEvent.
     */
    @Test
    public void testGetId()
    {
        System.out.println("getId");
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        Identifier expResult = new Identifier(instance.getIndex().databaseName(), instance.getIndex().tableName(), instance.getIndex().name(), instance.getUuid());
        Identifier result = instance.getId();
        assertEquals(expResult, result);
    }

    /**
     * Test of setId method, of class IndexCreatedEvent.
     */
    @Test
    public void testSetId()
    {
        System.out.println("setId");
        Identifier id = new Identifier();
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        boolean expectedExceptionThrown = false;
        try
        {
            instance.setId(id);
        } catch (UnsupportedOperationException e)
        {
            expectedExceptionThrown = true;
        }
        assertTrue("Expected exception not thrown", expectedExceptionThrown);
    }

    /**
     * Test of getDateStarted method, of class IndexCreatedEvent.
     */
    @Test
    public void testGetDateStarted()
    {
        System.out.println("getDateStarted");
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        Date expResult = instance.getDateStarted();
        Date result = instance.getDateStarted();
        assertEquals(expResult, result);//obvious
    }

    /**
     * Test of setDateStarted method, of class IndexCreatedEvent.
     */
    @Test
    public void testSetDateStarted() throws InterruptedException
    {
        System.out.println("setDateStarted");
        Date dateStarted = new Date();
        Thread.sleep(100);
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        instance.setDateStarted(dateStarted);
        assertEquals(dateStarted, instance.getDateStarted());
    }

    /**
     * Test of getStatusLastUpdatedAt method, of class IndexCreatedEvent.
     */
    @Test
    public void testGetStatusLastUpdatedAt()
    {
        System.out.println("getStatusLastUpdatedAt");
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        Date expResult = instance.getStatusLastUpdatedAt();
        Date result = instance.getStatusLastUpdatedAt();
        assertEquals(expResult, result);//obvious
    }

    /**
     * Test of setStatusLastUpdatedAt method, of class IndexCreatedEvent.
     */
    @Test
    public void testSetStatusLastUpdatedAt() throws InterruptedException
    {
        System.out.println("setStatusLastUpdatedAt");
        Date statusLastUpdatedAt = new Date();
        Thread.sleep(100);
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        instance.setStatusLastUpdatedAt(statusLastUpdatedAt);
        assertEquals(statusLastUpdatedAt, instance.getStatusLastUpdatedAt());
    }

    /**
     * Test of getEta method, of class IndexCreatedEvent.
     */
    @Test
    public void testGetEta() throws InterruptedException
    {
        System.out.println("getEta");
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();

        //just started test
        instance.setRecordsCompleted(1);
        instance.calculateValues();
        long result = instance.getEta();
        assertEquals(1, result, 2);

        //almost done test
        instance.setRecordsCompleted(1000);
        instance.calculateValues();
        result = instance.getEta();
        assertEquals(0, result, 2);

        //half way through test
        Thread.sleep(2000);//simulate some time passing
        instance.setRecordsCompleted(500);
        instance.calculateValues();
        result = instance.getEta();
        assertEquals(2, result, 2);
    }

    /**
     * Test of getIndex method, of class IndexCreatedEvent.
     */
    @Test
    public void testGetIndex()
    {
        System.out.println("getIndex");
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        Index expResult = Fixtures.createTestIndexOneField();
        Index result = instance.getIndex();
        assertEquals(expResult, result);
    }

    /**
     * Test of setIndex method, of class IndexCreatedEvent.
     */
    @Test
    public void testSetIndex()
    {
        System.out.println("setIndex");
        Index index = Fixtures.createTestIndexTwoField();
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        assertNotEquals(index, instance.getIndex());
        instance.setIndex(index);
        assertEquals(index, instance.getIndex());
    }

    /**
     * Test of getTotalRecords method, of class IndexCreatedEvent.
     */
    @Test
    public void testGetTotalRecords()
    {
        System.out.println("getTotalRecords");
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        long expResult = 1000L;
        long result = instance.getTotalRecords();
        assertEquals(expResult, result);
    }

    /**
     * Test of setTotalRecords method, of class IndexCreatedEvent.
     */
    @Test
    public void testSetTotalRecords()
    {
        System.out.println("setTotalRecords");
        long totalRecords = 2000L;
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        assertNotEquals(totalRecords, instance.getTotalRecords());
        instance.setTotalRecords(totalRecords);
        assertEquals(totalRecords, instance.getTotalRecords());
    }

    /**
     * Test of getRecordsCompleted method, of class IndexCreatedEvent.
     */
    @Test
    public void testGetRecordsCompleted()
    {
        System.out.println("getRecordsCompleted");
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        long expResult = 0;
        long result = instance.getRecordsCompleted();
        assertEquals(expResult, result);
    }

    /**
     * Test of setRecordsCompleted method, of class IndexCreatedEvent.
     */
    @Test
    public void testSetRecordsCompleted()
    {
        System.out.println("setRecordsCompleted");
        long recondsCompleted = 2000L;
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        assertNotEquals(recondsCompleted, instance.getRecordsCompleted());
        instance.setRecordsCompleted(recondsCompleted);
        assertEquals(recondsCompleted, instance.getRecordsCompleted());
    }

    /**
     * Test of hashCode method, of class IndexCreatedEvent.
     */
    @Test
    public void testHashCode()
    {
        System.out.println("hashCode");
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        int result = instance.hashCode();
        assertNotEquals(0, result);
        assertNotEquals(-1, result);
    }

    /**
     * Test of equals method, of class IndexCreatedEvent.
     */
    @Test
    public void testEquals()
    {
        System.out.println("equals");
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        boolean result = instance.equals(instance);
        assertTrue(result);
        Object obj = Fixtures.createTestIndexCreationStatusWithBulkDataHit();
        result = instance.equals(obj);
        assertFalse(result);

    }

    /**
     * Test of toString method, of class IndexCreatedEvent.
     */
    @Test
    public void testToString()
    {
        System.out.println("toString");
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();;
        String result = instance.toString();
        assertNotEquals("", result);
        assertNotNull(result);
    }

    /**
     * Test of getPrecentComplete method, of class IndexCreatedEvent.
     */
    @Test
    public void testGetPrecentComplete()
    {
        System.out.println("getPrecentComplete");
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        double expResult = 0.0;
        double result = instance.getPrecentComplete();
        assertEquals(expResult, result, 0.0);
        expResult = 50.0;
        instance.setRecordsCompleted(500);
        instance.calculateValues();
        result = instance.getPrecentComplete();
        assertEquals(expResult, result, 0.0);
        expResult = 100.0;
        instance.setRecordsCompleted(1000);
        instance.calculateValues();
        result = instance.getPrecentComplete();
        assertEquals(expResult, result, 0.0);
    }

    /**
     * Test of getError method, of class IndexCreatedEvent.
     */
    @Test
    public void testGetError()
    {
        System.out.println("getError");
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        String result = instance.getFatalError();
        assertNull(result);
    }

    /**
     * Test of setError method, of class IndexCreatedEvent.
     */
    @Test
    public void testSetError()
    {
        System.out.println("setError");
        String error = "New error";
        IndexCreatedEvent instance = Fixtures.createTestIndexCreationStatus();
        String result = instance.getFatalError();
        assertNull(result);
        instance.setFatalError(error);
        assertEquals(error, instance.getFatalError());
    }

}

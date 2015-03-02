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

import com.datastax.driver.core.Row;
import com.strategicgains.docussandra.domain.Database;
import java.util.List;
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
public class DatabaseRepositoryTest {
    
    public DatabaseRepositoryTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

   
    /**
     * Test of createEntity method, of class DatabaseRepository.
     */
    @Test
    @Ignore
    public void testCreateEntity() {
        System.out.println("createEntity");
        Database entity = null;
        DatabaseRepository instance = null;
        Database expResult = null;
        Database result = instance.createEntity(entity);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    

    /**
     * Test of updateEntity method, of class DatabaseRepository.
     */
    @Test
    @Ignore    
    public void testUpdateEntity() {
        System.out.println("updateEntity");
        Database entity = null;
        DatabaseRepository instance = null;
        Database expResult = null;
        Database result = instance.updateEntity(entity);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of deleteEntity method, of class DatabaseRepository.
     */
    @Test
    @Ignore
    public void testDeleteEntity() {
        System.out.println("deleteEntity");
        Database entity = null;
        DatabaseRepository instance = null;
        instance.deleteEntity(entity);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of readAll method, of class DatabaseRepository.
     */
    @Test
    @Ignore
    public void testReadAll() {
        System.out.println("readAll");
        DatabaseRepository instance = null;
        List<Database> expResult = null;
        List<Database> result = instance.readAll();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    
}

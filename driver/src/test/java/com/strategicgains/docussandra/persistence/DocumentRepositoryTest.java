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
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.repoexpress.domain.Identifier;
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
public class DocumentRepositoryTest {
    
    public DocumentRepositoryTest() {
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
     * Test of doCreate method, of class DocumentRepository.
     */
    @Test
    @Ignore
    public void testDoCreate() {
        System.out.println("doCreate");
        Document entity = null;
        DocumentRepository instance = null;
        Document expResult = null;
        Document result = instance.doCreate(entity);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of doRead method, of class DocumentRepository.
     */
    @Test
    @Ignore
    public void testDoRead() {
        System.out.println("doRead");
        Identifier identifier = null;
        DocumentRepository instance = null;
        Document expResult = null;
        Document result = instance.doRead(identifier);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of doUpdate method, of class DocumentRepository.
     */
    @Test
    @Ignore
    public void testDoUpdate() {
        System.out.println("doUpdate");
        Document entity = null;
        DocumentRepository instance = null;
        Document expResult = null;
        Document result = instance.doUpdate(entity);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of doDelete method, of class DocumentRepository.
     */
    @Test
    @Ignore
    public void testDoDelete() {
        System.out.println("doDelete");
        Document entity = null;
        DocumentRepository instance = null;
        instance.doDelete(entity);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of exists method, of class DocumentRepository.
     */
    @Test
    @Ignore
    public void testExists() {
        System.out.println("exists");
        Identifier identifier = null;
        DocumentRepository instance = null;
        boolean expResult = false;
        boolean result = instance.exists(identifier);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    
}

package com.strategicgains.docussandra;

import com.strategicgains.docussandra.domain.Index;
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
public class UtilsTest {
    
    public UtilsTest() {
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
     * Test of calculateITableName method, of class Utils.
     */
    @Test
    public void testCalculateITableName_3args() {
        System.out.println("calculateITableName");
        String databaseName = "myDb";
        String tableName = "myTable";
        String indexName = "yoIndex";
        String expResult = "myDb_myTable_yoIndex";
        String result = Utils.calculateITableName(databaseName, tableName, indexName);
        assertEquals(expResult, result);
    }

    /**
     * Test of calculateITableName method, of class Utils.
     */
    @Test
    public void testCalculateITableName_Index() {
        System.out.println("calculateITableName");
        String databaseName = "myDb";
        String tableName = "myTable";
        String indexName = "yoIndex";       
        Index index = new Index(indexName);
        index.table(databaseName, tableName);
        String expResult = "myDb_myTable_yoIndex";
        String result = Utils.calculateITableName(index);
        assertEquals(expResult, result);
    }
    
}

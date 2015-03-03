package com.strategicgains.docussandra.persistence;

import com.strategicgains.docussandra.Utils;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.testhelper.Fixtures;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author udeyoje
 */
public class ITableRepositoryTest
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private Fixtures f;

    public ITableRepositoryTest()
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
        f.clearTestTables();
    }

    @After
    public void tearDown()
    {
        f.clearTestTables();
    }

    /**
     * Test of iTableExists method, of class ITableDao.
     */
    @Test
    public void testITableExists()
    {
        System.out.println("iTableExists");
        Index index = Fixtures.createTestIndexOneField();
        ITableRepository cleanUpInstance = new ITableRepository(f.getSession());
        boolean expResult = false;//Note: Negative Test Only
        boolean result = cleanUpInstance.iTableExists(index);
        assertEquals(expResult, result);
    }

    /**
     * Test of createITable method, of class ITableDao.
     */
    @Test
    public void testCreateITable()
    {
        System.out.println("createITable");
        Index index = Fixtures.createTestIndexOneField();
        ITableRepository instance = new ITableRepository(f.getSession());
        boolean result = instance.iTableExists(index);
        assertEquals(false, result);//make sure it doesn't exist yet

        instance.createITable(index);
        result = instance.iTableExists(index);
        assertEquals(true, result);

        Index index2 = Fixtures.createTestIndexTwoField();
        result = instance.iTableExists(index2);
        assertEquals(false, result);//make sure it doesn't exist yet

        instance.createITable(index2);
        result = instance.iTableExists(index2);
        assertEquals(true, result);
    }

    /**
     * Test of generateTableCreationSyntax method, of class ITableDao.
     */
    @Test
    public void testGenerateTableCreationSyntax()
    {
        System.out.println("generateTableCreationSyntax");
        ITableRepository instance = new ITableRepository(f.getSession());
        String response = instance.generateTableCreationSyntax(Fixtures.createTestIndexOneField());
        Assert.assertNotNull(response);
        assertEquals("CREATE TABLE docussandra.mydb_mytable_myindexwithonefield (bucket varchar, id uuid, object blob, created_at timestamp, updated_at timestamp, myindexedfield varchar, PRIMARY KEY ((bucket), myindexedfield, id));", response);
        response = instance.generateTableCreationSyntax(Fixtures.createTestIndexTwoField());
        Assert.assertNotNull(response);
        assertEquals("CREATE TABLE docussandra.mydb_mytable_myindexwithtwofields (bucket varchar, id uuid, object blob, created_at timestamp, updated_at timestamp, myindexedfield1 varchar, myindexedfield2 varchar, PRIMARY KEY ((bucket), myindexedfield1, myindexedfield2, id));", response);
    }

    /**
     * Test of generateTableCreationSyntax method, of class ITableDao.
     */
    @Test
    public void testGenerateTableCreationSyntaxUnique()
    {
        System.out.println("testGenerateTableCreationSyntaxUnique");
        ITableRepository instance = new ITableRepository(f.getSession());
        Index one = Fixtures.createTestIndexOneField();
        one.isUnique(true);
        String response = instance.generateTableCreationSyntax(one);
        Assert.assertNotNull(response);
        assertEquals("CREATE TABLE docussandra.mydb_mytable_myindexwithonefield (bucket varchar, id uuid, object blob, created_at timestamp, updated_at timestamp, myindexedfield varchar, PRIMARY KEY ((bucket), myindexedfield));", response);
        Index two = Fixtures.createTestIndexTwoField();
        two.isUnique(true);
        response = instance.generateTableCreationSyntax(two);
        Assert.assertNotNull(response);
        assertEquals("CREATE TABLE docussandra.mydb_mytable_myindexwithtwofields (bucket varchar, id uuid, object blob, created_at timestamp, updated_at timestamp, myindexedfield1 varchar, myindexedfield2 varchar, PRIMARY KEY ((bucket), myindexedfield1, myindexedfield2));", response);
    }

    /**
     * Test of deleteITable method, of class ITableDao.
     */
    @Test
    public void testDeleteITable()
    {
        System.out.println("deleteITable");
        ITableRepository instance = new ITableRepository(f.getSession());
        Index index = Fixtures.createTestIndexOneField();
        boolean result = instance.iTableExists(index);
        assertEquals(false, result);//not here        
        instance.createITable(index);
        result = instance.iTableExists(index);
        assertEquals(true, result);//now it's here
        instance.deleteITable(index);
        result = instance.iTableExists(index);
        assertEquals(false, result);//now it's not
    }

    /**
     * Test of deleteITable method, of class ITableDao.
     */
    @Test
    public void testDeleteITable_String()
    {
        System.out.println("deleteITable");
        ITableRepository instance = new ITableRepository(f.getSession());
        Index index = Fixtures.createTestIndexOneField();
        boolean result = instance.iTableExists(index);
        assertEquals(false, result);//not here        
        instance.createITable(index);
        result = instance.iTableExists(index);
        assertEquals(true, result);//now it's here
        instance.deleteITable(Utils.calculateITableName(index));
        result = instance.iTableExists(index);
        assertEquals(false, result);//now it's not
    }

}

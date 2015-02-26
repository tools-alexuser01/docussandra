package com.strategicgains.docussandra.persistence;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.strategicgains.docussandra.Utils;
import com.strategicgains.docussandra.bucketmanagement.SimpleIndexBucketLocatorImpl;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.util.ArrayList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author udeyoje
 */
public class ITableDaoTest {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private Session session;

    public static final String DB = "mydb";

    public ITableDaoTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        Fixtures f = Fixtures.getInstance();
        Cluster cluster = Cluster.builder().addContactPoints(f.getCassandraSeeds()).build();
        final Metadata metadata = cluster.getMetadata();
        session = cluster.connect(f.getCassandraKeyspace());
        logger.info("Connected to cluster: " + metadata.getClusterName() + '\n');
        clearTestITables();
    }

    private void clearTestITables() {
        ITableDao cleanUpInstance = new ITableDao(session);
        try {
            cleanUpInstance.deleteITable("mydb_mytable_myindexwithonefield");
        } catch (InvalidQueryException e) {
            logger.debug("Not dropping iTable, probably doesn't exist.");
        }
        try {
            cleanUpInstance.deleteITable("mydb_mytable_myindexwithtwofields");
        } catch (InvalidQueryException e) {
            logger.debug("Not dropping iTable, probably doesn't exist.");
        }
    }

    @After
    public void tearDown() {
        clearTestITables();
    }

    /**
     * Test of iTableExists method, of class ITableDao.
     */
    @Test
    public void testITableExists() {
        System.out.println("iTableExists");
        Index index = createTestIndexOneField();
        ITableDao cleanUpInstance = new ITableDao(session);
        boolean expResult = false;//Note: Negative Test Only
        boolean result = cleanUpInstance.iTableExists(index);
        assertEquals(expResult, result);
    }

    /**
     * Test of createITable method, of class ITableDao.
     */
    @Test
    public void testCreateITable() {
        System.out.println("createITable");
        Index index = createTestIndexOneField();
        ITableDao instance = new ITableDao(session);
        boolean result = instance.iTableExists(index);
        assertEquals(false, result);//make sure it doesn't exist yet

        instance.createITable(index);
        result = instance.iTableExists(index);
        assertEquals(true, result);

        Index index2 = createTestIndexTwoField();
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
    public void testGenerateTableCreationSyntax() {
        System.out.println("generateTableCreationSyntax");
        ITableDao instance = new ITableDao(session);
        String response = instance.generateTableCreationSyntax(createTestIndexOneField());
        Assert.assertNotNull(response);
        assertEquals("CREATE TABLE docussandra.mydb_mytable_myindexwithonefield (bucket varchar, id uuid, object blob, created_at timestamp, updated_at timestamp, myindexedfield varchar, PRIMARY KEY ((bucket), myindexedfield));", response);
        response = instance.generateTableCreationSyntax(createTestIndexTwoField());
        Assert.assertNotNull(response);
        assertEquals("CREATE TABLE docussandra.mydb_mytable_myindexwithtwofields (bucket varchar, id uuid, object blob, created_at timestamp, updated_at timestamp, myindexedfield1 varchar, myindexedfield2 varchar, PRIMARY KEY ((bucket), myindexedfield1, myindexedfield2));", response);
    }

    /**
     * Test of deleteITable method, of class ITableDao.
     */
    @Test
    public void testDeleteITable() {
        System.out.println("deleteITable");
        ITableDao instance = new ITableDao(session);
        Index index = createTestIndexOneField();
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
    public void testDeleteITable_String() {
        System.out.println("deleteITable");
        ITableDao instance = new ITableDao(session);
        Index index = createTestIndexOneField();
        boolean result = instance.iTableExists(index);
        assertEquals(false, result);//not here        
        instance.createITable(index);
        result = instance.iTableExists(index);
        assertEquals(true, result);//now it's here
        instance.deleteITable(Utils.calculateITableName(index));
        result = instance.iTableExists(index);
        assertEquals(false, result);//now it's not
    }

    /**
     * Creates at test index with one field.
     *
     * @return
     */
    //TODO: move to a TestHelper class
    public static final Index createTestIndexOneField() {
        Index index = new Index("myIndexWithOneField");
        index.table(DB, "mytable");
        ArrayList<String> fields = new ArrayList<>();
        fields.add("myindexedfield");
        index.fields(fields);
        index.isUnique(false);
        return index;
    }

    /**
     * Creates at test index with two fields.
     *
     * @return
     */
    //TODO: move to a TestHelper class
    public static final Index createTestIndexTwoField() {
        Index index = new Index("myindexwithtwofields");
        index.table(DB, "mytable");
        ArrayList<String> fields = new ArrayList<>();
        fields.add("myindexedfield1");
        fields.add("myindexedfield2");
        index.fields(fields);
        index.isUnique(true);
        return index;
    }
}

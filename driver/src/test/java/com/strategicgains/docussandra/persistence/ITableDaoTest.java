package com.strategicgains.docussandra.persistence;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.testhelper.Fixtures;
import org.junit.After;
import org.junit.AfterClass;
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
        session = cluster.connect();
        logger.info("Connected to cluster: "+metadata.getClusterName()+'\n');   
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of iTableExists method, of class ITableDao.
     */
    @Test
    public void testITableExists() {
        System.out.println("iTableExists");
        Index index = new Index("myIndexName");
        index.table("myDB", "myTable");
        ITableDao instance = new ITableDao(session);
        boolean expResult = false;
        boolean result = instance.iTableExists(index);
        assertEquals(expResult, result);
    }

    /**
     * Test of createITable method, of class ITableDao.
     */
    @Ignore   
    @Test
    public void testCreateITable() {
        System.out.println("createITable");
        Index index = null;
        ITableDao instance = null;
        instance.createITable(index);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of deleteITable method, of class ITableDao.
     */
    @Test
    @Ignore
    public void testDeleteITable() {
        System.out.println("deleteITable");
        Index index = null;
        ITableDao instance = null;
        instance.deleteITable(index);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}

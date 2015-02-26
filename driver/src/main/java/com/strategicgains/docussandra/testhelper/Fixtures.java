package com.strategicgains.docussandra.testhelper;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.ParsedQuery;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.domain.WhereClause;
import com.strategicgains.docussandra.persistence.DocumentRepository;
import com.strategicgains.docussandra.persistence.ITableDao;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.TableRepository;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Used for testing only! Allows us to quickly establish a database connection
 * and work with test data.
 *
 * @author udeyoje
 */
public class Fixtures {

    private static final Fixtures INSTANCE = new Fixtures();
    private static final String CASSANDRA_SEEDS = "cassandra.seeds";
    private static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
    public static final String DB = "mydb";

    private Session session;
    private final String[] cassandraSeeds;
    private final String cassandraKeyspace;


    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Private constructor as this is a singleton object
     */
    private Fixtures() {
        //try {
        //Properties properties = loadTestProperties(); //TODO: put this call back in
        String cassandraSeedsProperty = "127.0.0.1";//properties.getProperty(
        //CASSANDRA_SEEDS, "localhost");
        cassandraKeyspace = "docussandra";//properties.getProperty(CASSANDRA_KEYSPACE);
        cassandraSeeds = cassandraSeedsProperty.split(",");
//        } catch (IOException ioe) { // Because Checked Exceptions are the bane
//            throw new RuntimeException(ioe);
//        }
        Cluster cluster = Cluster.builder().addContactPoints(this.getCassandraSeeds()).build();
        final Metadata metadata = cluster.getMetadata();
        session = cluster.connect(this.getCassandraKeyspace());
        logger.info("Connected to cluster: " + metadata.getClusterName() + '\n');
    }

    /**

     * Get this singleton instance. THIS CLASS IS FOR TESTING ONLY.
     *
     * @return the singleton instance
     */
    public static Fixtures getInstance() {
        return INSTANCE;
    }

    public String[] getCassandraSeeds() {
        return cassandraSeeds;
    }

    public String getCassandraKeyspace() {
        return cassandraKeyspace;
    }

    /**
     * Load properties from a property file
     */
    private Properties loadTestProperties() throws IOException {
        FileInputStream fis = null;
        try {
            String testEnv = System.getProperty("TEST_ENV") != null ? System.getProperty("TEST_ENV") : "local";
            File envFile = new File("config/" + testEnv + "/environment.properties");
            Properties properties = new Properties();
            fis = new FileInputStream(envFile);
            properties.load(fis);
            return properties;
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException e) {
                // too late to care at this point
            }
        }
    }

    /**
     * Creates at test index with two fields.
     *
     * @return
     */
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

    /**
     * Creates at test index with one field.
     *
     * @return
     */
    public static final Index createTestIndexOneField() {
        Index index = new Index("myIndexWithOneField");
        index.table(DB, "mytable");
        ArrayList<String> fields = new ArrayList<>();
        fields.add("myindexedfield");
        index.fields(fields);
        index.isUnique(false);
        return index;
    }

    public void clearTestTables() {
        ITableDao cleanUpInstance = new ITableDao(getSession());
        IndexRepository indexRepo = new IndexRepository(getSession());

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
        try {
            DocumentRepository docRepo = new DocumentRepository(getSession());
            docRepo.delete(Fixtures.createTestDocument());
            docRepo.delete(Fixtures.createTestDocument2());
        } catch (InvalidQueryException e) {
            logger.debug("Not dropping document, probably doesn't exist.");
        }
        try {
            TableRepository tableRepo = new TableRepository(getSession());
            tableRepo.delete(Fixtures.createTestTable());
        } catch (InvalidQueryException e) {
            logger.debug("Not dropping table, probably doesn't exist.");
        }
        try {
            indexRepo.delete(Fixtures.createTestIndexOneField());
        } catch (InvalidQueryException e) {
            logger.debug("Not dropping table, probably doesn't exist.");
        }
        try {
            indexRepo.delete(Fixtures.createTestIndexTwoField());
        } catch (InvalidQueryException e) {
            logger.debug("Not deleting index, probably doesn't exist.");
        }
    }

    public void createTestTables() {
        System.out.println("createTestITables");
        ITableDao iTableDao = new ITableDao(getSession());
        Index index = Fixtures.createTestIndexOneField();
        Index index2 = Fixtures.createTestIndexTwoField();
        IndexRepository indexRepo = new IndexRepository(getSession());
        indexRepo.create(index);
        //indexRepo.create(index2);
        iTableDao.createITable(index2);
        TableRepository tableRepo = new TableRepository(getSession());
        tableRepo.create(Fixtures.createTestTable());
    }

    public static final Document createTestDocument() {
        Document entity = new Document();
        entity.table("mydb", "mytable");
        entity.object("{'greeting':'hello', 'myindexedfield': 'thisismyfield', 'myindexedfield1':'my second field', 'myindexedfield2':'my third field'}");
        entity.setUuid(new UUID(0L, 1L));
        entity.setCreatedAt(new Date());
        entity.setUpdatedAt(new Date());
        return entity;
    }

    /**
     * Creates a test document.
     *
     * @return
     */
    public static final Document createTestDocument2() {
        Document entity = new Document();
        entity.table("mydb", "mytable");
        entity.object("{'greeting':'hello', 'myindexedfield': 'this is my field', 'myindexedfield1':'my second field', 'myindexedfield2':'my third field'}");
        entity.setUuid(new UUID(0L, 1L));
        entity.setCreatedAt(new Date());
        entity.setUpdatedAt(new Date());
        return entity;
    }

    /**
     * Creates a simple query based on a single index for testing.
     *
     * @return
     */
    public static final Query createTestQuery() {
        Query query = new Query();
        query.setWhere("myindexedfield = 'thisismyfield'");
        query.setTable("mytable");
        return query;
    }
    
        /**
     * Creates a simple query based on a single index for testing.
     *
     * @return
     */
    public static final Query createTestQuery2() {
        Query query = new Query();
        query.setWhere("myindexedfield1 = 'thisismyfield' AND myindexedfield2 = 'blah'");
        query.setTable("mytable");
        return query;
    }

    /**
     * Creates a simple parsed query based on a single index for testing.
     *
     * @return
     */
    public static final ParsedQuery createTestParsedQuery() {
        Query query = createTestQuery();
        WhereClause whereClause = new WhereClause(query.getWhere());
        String iTable = "mydb_mytable_myindexwithonefield";
        return new ParsedQuery(query, whereClause, iTable);
    }

    /**
     * Creates a simple parsed query based on a single index for testing.
     *
     * @return
     */
    public static final ParsedQuery createTestParsedQuery2() {
        Query query = new Query();
        query.setWhere("myindexedfield = 'foo'");
        query.setTable("mytable");
        WhereClause whereClause = new WhereClause(query.getWhere());
        String iTable = "mydb_mytable_myindexwithonefield";
        return new ParsedQuery(query, whereClause, iTable);
    }

    /**
     * Creates a simple table for testing.
     *
     * @return
     */
    public static final Table createTestTable() {
        Table t = new Table();
        t.name("mytable");
        t.database(Fixtures.DB);
        return t;
    }

    /**
     * @return the session
     */
    public Session getSession() {
        return session;
    }
}

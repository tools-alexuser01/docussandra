package com.strategicgains.docussandra.testhelper;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.ParsedQuery;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.domain.WhereClause;
import com.strategicgains.docussandra.persistence.DatabaseRepository;
import com.strategicgains.docussandra.persistence.DocumentRepository;
import com.strategicgains.docussandra.persistence.ITableRepository;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.TableRepository;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for testing only! Allows us to quickly establish a database connection
 * and work with test data.
 *
 * @author udeyoje
 */
public class Fixtures
{

    private static Fixtures INSTANCE = null;
    private static final String CASSANDRA_SEEDS = "cassandra.seeds";
    private static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
    public static final String DB = "mydb";
    private static List<Document> bulkDocs = null;

    private Session session;
    private final String[] cassandraSeeds;
    private final String cassandraKeyspace;

    private static Logger logger = LoggerFactory.getLogger(Fixtures.class);

    /**
     * Private constructor as this is a singleton object
     */
    private Fixtures()
    {
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
    public static Fixtures getInstance()
    {
        if (INSTANCE == null)
        {
            INSTANCE = new Fixtures();
        }
        return INSTANCE;
    }

    public String[] getCassandraSeeds()
    {
        return cassandraSeeds;
    }

    public String getCassandraKeyspace()
    {
        return cassandraKeyspace;
    }

    /**
     * Load properties from a property file
     */
    private Properties loadTestProperties() throws IOException
    {
        FileInputStream fis = null;
        try
        {
            String testEnv = System.getProperty("TEST_ENV") != null ? System.getProperty("TEST_ENV") : "local";
            File envFile = new File("config/" + testEnv + "/environment.properties");
            Properties properties = new Properties();
            fis = new FileInputStream(envFile);
            properties.load(fis);
            return properties;
        } finally
        {
            try
            {
                if (fis != null)
                {
                    fis.close();
                }
            } catch (IOException e)
            {
                // too late to care at this point
            }
        }
    }

    public static List<Document> getBulkDocuments() throws IOException, ParseException
    {
        if (bulkDocs == null)
        {
            JSONParser parser = new JSONParser();
            logger.info("Data path: " + new File("./src/test/resources/documents.json").getAbsolutePath());
            JSONObject jsonObject = (JSONObject) parser.parse(new FileReader("./src/test/resources/documents.json"));
            JSONArray docs = (JSONArray) jsonObject.get("documents");
            List<Document> toReturn = new ArrayList<>(docs.size());
            for (int i = 0; i < docs.size(); i++)
            {
                Document doc = new Document();
                doc.table(createTestTable());
                doc.setUuid(new UUID(Long.MAX_VALUE - i, 1));//give it a UUID that we will reconize
                JSONObject object = (JSONObject)docs.get(i);
                doc.object(object.toJSONString());
                toReturn.add(doc);
            }
            bulkDocs = toReturn;
        }
        return bulkDocs;
    }

    /**
     * Creates at test index with two fields.
     *
     * @return
     */
    public static final Index createTestIndexTwoField()
    {
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
    public static final Index createTestIndexOneField()
    {
        Index index = new Index("myindexwithonefield");
        index.table(DB, "mytable");
        ArrayList<String> fields = new ArrayList<>();
        fields.add("myindexedfield");
        index.fields(fields);
        index.isUnique(false);
        return index;
    }

    /**
     * Creates at test index with one field that will hit every row of our bulk
     * data.
     *
     * @return
     */
    public static final Index createTestIndexWithBulkDataHit()
    {
        Index index = new Index("myindexbulkdata");
        index.table(DB, "mytable");
        ArrayList<String> fields = new ArrayList<>();
        fields.add("field1");
        index.fields(fields);
        index.isUnique(false);
        return index;
    }

    public void insertIndex(Index index)
    {
        IndexRepository indexRepo = new IndexRepository(session);
        indexRepo.create(index);
    }

    public void clearTestTables()
    {
        ITableRepository cleanUpInstance = new ITableRepository(getSession());
        IndexRepository indexRepo = new IndexRepository(getSession());
        DatabaseRepository databaseRepo = new DatabaseRepository(getSession());
        DocumentRepository docRepo = new DocumentRepository(getSession());
        TableRepository tableRepo = new TableRepository(getSession());
        try
        {
            cleanUpInstance.deleteITable("mydb_mytable_myindexwithonefield");
        } catch (InvalidQueryException e)
        {
            logger.debug("Not dropping iTable, probably doesn't exist.");
        }
        try
        {
            cleanUpInstance.deleteITable("mydb_mytable_myindexwithtwofields");
        } catch (InvalidQueryException e)
        {
            logger.debug("Not dropping iTable, probably doesn't exist.");
        }
        try
        {
            cleanUpInstance.deleteITable("mydb_mytable_myindexbulkdata");
        } catch (InvalidQueryException e)
        {
            logger.debug("Not dropping iTable, probably doesn't exist.");
        }
        try
        {

            docRepo.delete(Fixtures.createTestDocument());
            docRepo.delete(Fixtures.createTestDocument2());
        } catch (InvalidQueryException e)
        {
            logger.debug("Not dropping document, probably doesn't exist.");
        }
        try
        {
            List<Document> toDelete = getBulkDocuments();
            for (Document d : toDelete)
            {
                try
                {
                    docRepo.delete(d);
                } catch (InvalidQueryException e)
                {
                    logger.debug("Not dropping bulk document, probably doesn't exist.");
                }
            }
        } catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        try
        {

            tableRepo.delete(Fixtures.createTestTable());
        } catch (InvalidQueryException e)
        {
            logger.debug("Not dropping table, probably doesn't exist.");
        }
        try
        {
            indexRepo.delete(Fixtures.createTestIndexOneField());
        } catch (InvalidQueryException e)
        {
            logger.debug("Not dropping table, probably doesn't exist.");
        }
        try
        {
            indexRepo.delete(Fixtures.createTestIndexTwoField());
        } catch (InvalidQueryException e)
        {
            logger.debug("Not deleting index, probably doesn't exist.");
        }
        try
        {
            indexRepo.delete(Fixtures.createTestIndexWithBulkDataHit());
        } catch (InvalidQueryException e)
        {
            logger.debug("Not deleting index, probably doesn't exist.");
        }
        try
        {
            databaseRepo.delete(Fixtures.createTestDatabase());
        } catch (InvalidQueryException e)
        {
            logger.debug("Not deleting database, probably doesn't exist.");
        }
    }

    public void createTestTables()
    {
        System.out.println("createTestITables");
        ITableRepository iTableDao = new ITableRepository(getSession());
        Index index = Fixtures.createTestIndexOneField();
        Index index2 = Fixtures.createTestIndexTwoField();
        Index index3 = Fixtures.createTestIndexWithBulkDataHit();
        IndexRepository indexRepo = new IndexRepository(getSession());
        indexRepo.create(index);
        //indexRepo.create(index2);
        iTableDao.createITable(index2);
        iTableDao.createITable(index3);
        TableRepository tableRepo = new TableRepository(getSession());
        tableRepo.create(Fixtures.createTestTable());
    }

    public static final Document createTestDocument()
    {
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
    public static final Document createTestDocument2()
    {
        Document entity = new Document();
        entity.table("mydb", "mytable");
        entity.object("{'greeting':'hello', 'myindexedfield': 'this is my field', 'myindexedfield1':'my second field', 'myindexedfield2':'my third field'}");
        entity.setUuid(new UUID(0L, 2L));
        entity.setCreatedAt(new Date());
        entity.setUpdatedAt(new Date());
        return entity;
    }

    public void insertDocument(Document document)
    {
        DocumentRepository documentRepo = new DocumentRepository(getSession());
        documentRepo.create(document);
    }

    public void insertDocuments(List<Document> documents)
    {
        DocumentRepository documentRepo = new DocumentRepository(getSession());
        for (Document document : documents)
        {
            documentRepo.create(document);
        }
    }

    public void deleteDocument(Document document)
    {
        DocumentRepository documentRepo = new DocumentRepository(getSession());
        documentRepo.delete(document);
    }

    /**
     * Creates a simple query based on a single index for testing.
     *
     * @return
     */
    public static final Query createTestQuery()
    {
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
    public static final Query createTestQuery2()
    {
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
    public static final ParsedQuery createTestParsedQuery()
    {
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
    public static final ParsedQuery createTestParsedQuery2()
    {
        Query query = new Query();
        query.setWhere("myindexedfield = 'foo'");
        query.setTable("mytable");
        WhereClause whereClause = new WhereClause(query.getWhere());
        String iTable = "mydb_mytable_myindexwithonefield";
        return new ParsedQuery(query, whereClause, iTable);
    }

    /**
     * Creates a simple parsed query based on a single index for testing.
     *
     * @return
     */
    public static final ParsedQuery createTestParsedQueryBulkData()
    {
        Query query = new Query();
        query.setWhere("field1 = 'this is my data'");
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
    public static final Table createTestTable()
    {
        Table t = new Table();
        t.name("mytable");
        t.database(Fixtures.DB);
        t.description("My Table stores a lot of data.");
        return t;
    }

    public void insertTable(Table table)
    {
        TableRepository tableRepo = new TableRepository(getSession());
        tableRepo.create(table);
    }

    public static Database createTestDatabase()
    {
        Database database = new Database(DB);
        database.description("This is a test database.");
        return database;
    }

    public void insertDatabase(Database database)
    {
        DatabaseRepository databaseRepo = new DatabaseRepository(getSession());
        databaseRepo.create(database);
    }

    /**
     * @return the session
     */
    public Session getSession()
    {
        return session;
    }
}

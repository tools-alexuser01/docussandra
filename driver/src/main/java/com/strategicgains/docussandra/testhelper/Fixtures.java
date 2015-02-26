package com.strategicgains.docussandra.testhelper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

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

    private final String[] cassandraSeeds;
    private final String cassandraKeyspace;

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
}

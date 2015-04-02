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
package com.strategicgains.docussandra.controller.perf;

import com.jayway.restassured.RestAssured;
import static com.jayway.restassured.RestAssured.given;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.util.Date;
import static org.hamcrest.Matchers.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import testhelper.RestExpressManager;

/**
 * Perf tests queries; pre-req: run the bulk loader first to load test data.
 *
 * @author udeyoje
 */
public class QueryControllerPerf
{

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryControllerPerf.class);
    private static final String BASE_URI = "http://localhost";
    private static final int PORT = 19080;
    private Fixtures f;

    public QueryControllerPerf() throws Exception
    {
        f = Fixtures.getInstance();
    }

    /**
     * Initialization that is performed once before any of the tests in this
     * class are executed.
     *
     * @throws Exception
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        RestAssured.baseURI = BASE_URI;
        RestAssured.port = PORT;
        //RestAssured.basePath = "/courses/" + COURSE_ID + "/categories";

//        String testEnv = System.getProperty("TEST_ENV") != null ? System.getProperty("TEST_ENV") : "local";
//        String[] env = {testEnv};
        //Thread.sleep(10000);
        RestExpressManager.getManager().ensureRestExpressRunning();
    }

    @Before
    public void beforeTest() throws Exception
    {
        RestAssured.basePath = "/players/players/queries";
    }

    /**
     * Cleanup that is performed once after all of the tests in this class are
     * executed.
     */
    @AfterClass
    public static void afterClass()
    {
    }

    /**
     * Cleanup that is performed after each test is executed.
     */
    @After
    public void afterTest()
    {
        
    }

    /**
     * Tests that the POST /{databases}/{table}/query endpoint properly runs a
     * query with a set time.
     */
    @Test
    public void postTableTest()
    {
        int numQueries = 1000;
        Date start = new Date();
        for (int i = 0; i < numQueries; i++)
        {
            LOGGER.debug("Query: " + i);
            given().header("limit", "10000").body("{\"where\":\"NAMELAST = 'Manning'\"}").expect().statusCode(200)
                    //.header("Location", startsWith(RestAssured.basePath + "/"))
                    .body("", notNullValue())
                    .body("id", notNullValue())
                    .when().post("");
        }
        Date end = new Date();
        long executionTime = end.getTime() - start.getTime();
        double inSeconds = executionTime / 1000;
        LOGGER.info("Time to execute for " + numQueries + " is: " + inSeconds + " seconds");
    }

    /**
     * Tests that the POST /{databases}/{table}/query endpoint properly runs a
     * two field query with a set time.
     */
    @Test
    public void postTableTestTwoField()
    {
        int numQueries = 1000;
        Date start = new Date();
        for (int i = 0; i < numQueries; i++)
        {
            LOGGER.debug("Query: " + i);
            given().header("limit", "10000").body("{\"where\":\"NAMELAST = 'Manning'AND NAMEFIRST = 'Peyton'\"}").expect().statusCode(200)
                    //.header("Location", startsWith(RestAssured.basePath + "/"))
                    .body("", notNullValue())
                    .body("id", notNullValue())
                    .when().post("");
        }
        Date end = new Date();
        long executionTime = end.getTime() - start.getTime();
        double inSeconds = executionTime / 1000;
        LOGGER.info("Time to execute two field test for " + numQueries + " is: " + inSeconds + " seconds");
    }
}

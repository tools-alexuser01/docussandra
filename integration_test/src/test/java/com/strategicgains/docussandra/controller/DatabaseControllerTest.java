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
package com.strategicgains.docussandra.controller;

import com.jayway.restassured.RestAssured;
import static com.jayway.restassured.RestAssured.expect;
import static com.jayway.restassured.RestAssured.given;
import com.strategicgains.docussandra.Main;
import com.strategicgains.docussandra.domain.Database;
import org.junit.BeforeClass;
import org.restexpress.RestExpress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.strategicgains.docussandra.testhelper.Fixtures;
import static org.hamcrest.Matchers.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author udeyoje
 */
public class DatabaseControllerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseControllerTest.class);
    private static final String BASE_URI = "http://localhost";
    private static final int PORT = 19080;
    private static RestExpress server;
    private Fixtures f;

    public DatabaseControllerTest() {
        f = Fixtures.getInstance();
    }

    /**
     * Initialization that is performed once before any of the tests in this
     * class are executed.
     *
     * @throws Exception
     */
    @BeforeClass
    public static void beforeClass() throws Exception {
        RestAssured.baseURI = BASE_URI;
        RestAssured.port = PORT;
        //RestAssured.basePath = "/courses/" + COURSE_ID + "/categories";

//        String testEnv = System.getProperty("TEST_ENV") != null ? System.getProperty("TEST_ENV") : "local";
//        String[] env = {testEnv};
        LOGGER.debug("Loading RestExpress Environment... ");

        server = Main.initializeServer(new String[0]);
    }

    @Before
    public void beforeTest() {
        f.clearTestTables();
    }

    /**
     * Cleanup that is performed once after all of the tests in this class are
     * executed.
     */
    @AfterClass
    public static void afterClass() {
        server.shutdown();
    }

    /**
     * Cleanup that is performed after each test is executed.
     */
    @After
    public void afterTest() {
        f.clearTestTables();
    }

//    /**
//     * Tests that the GET / properly retrieves all existing database
//     */
//    @Test
//    public void getDatabasesTest() {
//        Database testDb = Fixtures.createTestDatabase();
//        f.insertDatabase(testDb);
//        //not the best test
//        expect().statusCode(200)
//                .body("", contains(testDb.name()))
//                .body("", contains(testDb.description())).when()
//                .get("/");
//    }
    /**
     * Tests that the GET /{databases} properly retrieves an existing database
     */
    @Test
    public void getDatabaseTest() {
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        expect().statusCode(200)
                .body("name", equalTo(testDb.name()))
                .body("description", equalTo(testDb.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get("/" + testDb.getId());
    }

    /**
     * Tests that the POST /{databases} endpoint properly creates a database.
     */
    @Test
    public void postDatabaseTest() {
        Database testDb = Fixtures.createTestDatabase();
        String categoryStr = "{" + "\"description\" : \"" + testDb.description()
                + "\"," + "\"name\" : \"" + testDb.name() + "\"}";

        given().body(categoryStr).expect().statusCode(201)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("name", equalTo(testDb.name()))
                .body("description", equalTo(testDb.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post(testDb.name());
    }

}

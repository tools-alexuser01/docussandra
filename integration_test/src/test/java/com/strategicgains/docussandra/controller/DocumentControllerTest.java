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
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Document;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.strategicgains.docussandra.testhelper.Fixtures;
import static org.hamcrest.Matchers.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import testhelper.RestExpressManager;

/**
 *
 * @author udeyoje
 */
public class DocumentControllerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentControllerTest.class);
    private static final String BASE_URI = "http://localhost";
    private static final int PORT = 19080;
    private Fixtures f;

    public DocumentControllerTest() {
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
        //Thread.sleep(10000);
        RestExpressManager.getManager().ensureRestExpressRunning();
    }

    @Before
    public void beforeTest() {
        f.clearTestTables();
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        RestAssured.basePath = "/" + testDb.name();
    }

    /**
     * Cleanup that is performed once after all of the tests in this class are
     * executed.
     */
    @AfterClass
    public static void afterClass() {
    }

    /**
     * Cleanup that is performed after each test is executed.
     */
    @After
    public void afterTest() {
        f.clearTestTables();
    }

    /**
     * Tests that the GET /{databases}/{table} properly retrieves an existing
     * database.
     */
    @Test
    public void getDocumentTest() {
        Document testDocument = Fixtures.createTestDocument();
        f.insertDocument(testDocument);
        expect().statusCode(200)
                .body("id", equalTo(testDocument.getId().toString()))
                .body("object", containsString(testDocument.object()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(testDocument.getId().toString());
    }

//    /**
//     * Tests that the POST /{databases}/{table} endpoint properly creates a
//     * table.
//     */
//    @Test
//    public void postDocumentTest() {
//        Document testDocument = Fixtures.createTestDocument();
//        String tableStr = "{" + "\"description\" : \"" + testDocument.description()
//                + "\"," + "\"name\" : \"" + testDocument.name() + "\"}";
//        //act
//        given().body(tableStr).expect().statusCode(201)
//                //.header("Location", startsWith(RestAssured.basePath + "/"))
//                .body("name", equalTo(testDocument.name()))
//                .body("description", equalTo(testDocument.description()))
//                .body("createdAt", notNullValue())
//                .body("updatedAt", notNullValue())
//                .when().post(testDocument.name());
//        //check
//        expect().statusCode(200)
//                .body("name", equalTo(testDocument.name()))
//                .body("description", equalTo(testDocument.description()))
//                .body("createdAt", notNullValue())
//                .body("updatedAt", notNullValue()).when()
//                .get(testDocument.name());
//    }
//
//    /**
//     * Tests that the PUT /{databases}/{table} endpoint properly updates a
//     * table.
//     */
//    @Test
//    public void putDocumentTest() {
//        Document testDocument = Fixtures.createTestDocument();
//        f.insertDocument(testDocument);
//        String newDesciption = "this is a new description";
//        String tableStr = "{" + "\"description\" : \"" + newDesciption
//                + "\"," + "\"name\" : \"" + testDocument.name() + "\"}";
//
//        //act
//        given().body(tableStr).expect().statusCode(204)
//                .when().put(testDocument.name());
//
//        //check
//        expect().statusCode(200)
//                .body("name", equalTo(testDocument.name()))
//                .body("description", equalTo(newDesciption))
//                .body("createdAt", notNullValue())
//                .body("updatedAt", notNullValue()).when()
//                .get(testDocument.name());
//    }
//
//    /**
//     * Tests that the DELETE /{databases}/{table} endpoint properly deletes a
//     * table.
//     */
//    @Test
//    public void deleteDocumentTest() {
//        Document testDocument = Fixtures.createTestDocument();
//        f.insertDocument(testDocument);
//        //act
//        given().expect().statusCode(204)
//                .when().delete(testDocument.name());
//        //check
//        expect().statusCode(404).when()
//                .get(testDocument.name());
//    }
}

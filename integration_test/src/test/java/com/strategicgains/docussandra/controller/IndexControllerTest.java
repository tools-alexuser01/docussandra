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
import com.jayway.restassured.parsing.Parser;
import com.jayway.restassured.response.Response;
import com.mongodb.util.JSON;
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.persistence.IndexRepository;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.strategicgains.docussandra.testhelper.Fixtures;
import com.strategicgains.repoexpress.domain.Identifier;
import java.util.ArrayList;
import java.util.UUID;
import org.bson.BSONObject;
import static org.hamcrest.Matchers.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import testhelper.RestExpressManager;

/**
 *
 * @author udeyoje
 */
public class IndexControllerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexControllerTest.class);
    private static final String BASE_URI = "http://localhost";
    private static final int PORT = 19080;
    private Fixtures f;

    public IndexControllerTest() {
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
        RestExpressManager.getManager().ensureRestExpressRunning();
    }

    @Before
    public void beforeTest() {
        f.clearTestTables();
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        Table testTable = Fixtures.createTestTable();
        f.insertTable(testTable);
        RestAssured.basePath = "/" + testDb.name() + "/" + testTable.name() + "/indexes";
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
     * Tests that the GET /{databases}/{table}/indexes/{index} properly
     * retrieves an existing index.
     */
    @Test
    public void getIndexTest() {
        Index testIndex = Fixtures.createTestIndexOneField();
        f.insertIndex(testIndex);
        expect().statusCode(200)
                .body("name", equalTo(testIndex.name()))
                .body("fields", notNullValue())
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(testIndex.name());
    }

    /**
     * Tests that the POST /{databases}/{table}/indexes/ endpoint properly
     * creates a index.
     */
    @Test
    public void postIndexTest() {
        Index testIndex = Fixtures.createTestIndexOneField();
        String tableStr = "{" + "\"fields\" : [\"" + testIndex.fields().get(0)
                + "\"]," + "\"name\" : \"" + testIndex.name() + "\"}";

        //act
        given().body(tableStr).expect().statusCode(201)
                .body("name", equalTo(testIndex.name()))
                .body("fields", notNullValue())
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post("/" + testIndex.name()).andReturn();

        //check
        expect().statusCode(200)
                .body("name", equalTo(testIndex.name()))
                .body("fields", notNullValue())
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .get(testIndex.name());
    }


    /**
     * Tests that the DELETE /{databases}/{table}/indexes/{index} endpoint
     * properly deletes a index.
     */
    @Test
    public void deleteIndexTest() {
        Index testIndex = Fixtures.createTestIndexOneField();
        f.insertIndex(testIndex);
        //act
        given().expect().statusCode(204)
                .when().delete(testIndex.name());
        //check
        expect().statusCode(404).when()
                .get(testIndex.name());
    }
}

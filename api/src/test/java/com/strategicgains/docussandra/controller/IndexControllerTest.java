package com.strategicgains.docussandra.controller;

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
import com.jayway.restassured.RestAssured;
import static com.jayway.restassured.RestAssured.expect;
import static com.jayway.restassured.RestAssured.given;
import com.jayway.restassured.response.ResponseOptions;
import com.strategicgains.docussandra.cache.CacheFactory;
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.persistence.DocumentRepository;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.util.List;
import org.apache.commons.lang3.time.StopWatch;
import static org.hamcrest.Matchers.*;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import testhelper.RestExpressManager;

/**
 *
 * @author udeyoje
 */
public class IndexControllerTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexControllerTest.class);
    private static final String BASE_URI = "http://localhost";
    private static final int PORT = 19080;
    private static Fixtures f;
    private JSONParser parser = new JSONParser();

    public IndexControllerTest() throws Exception
    {
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
        f = Fixtures.getInstance();
        RestExpressManager.getManager().ensureRestExpressRunning();
    }

    @Before
    public void beforeTest()
    {
        f.clearTestTables();
        CacheFactory.clearAllCaches();
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
    public static void afterClass()
    {
        f.clearTestTables();
    }

    /**
     * Cleanup that is performed after each test is executed.
     */
    @After
    public void afterTest()
    {

    }

    /**
     * Tests that the GET /{databases}/{table}/indexes/{index} properly
     * retrieves an existing index.
     */
    @Test
    public void getIndexTest()
    {
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
    public void postIndexTest() throws InterruptedException
    {
        Index testIndex = Fixtures.createTestIndexOneField();
        String tableStr = "{" + "\"fields\" : [\"" + testIndex.fields().get(0)
                + "\"]," + "\"name\" : \"" + testIndex.name() + "\"}";

        //act
        given().body(tableStr).expect().statusCode(201)
                .body("index.name", equalTo(testIndex.name()))
                .body("index.fields", notNullValue())
                .body("index.createdAt", notNullValue())
                .body("index.updatedAt", notNullValue())
                .body("index.active", equalTo(false))
                .body("id", notNullValue())
                .body("dateStarted", notNullValue())
                .body("statusLastUpdatedAt", notNullValue())
                .body("totalRecords", equalTo(0))
                .body("recordsCompleted", equalTo(0))
                .when().post("/" + testIndex.name());

        Thread.sleep(100);//sleep for a hair to let the indexing complete

        //check self (index endpoint)
        expect().statusCode(200)
                .body("name", equalTo(testIndex.name()))
                .body("fields", notNullValue())
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .body("active", equalTo(true))
                .get("/" + testIndex.name());
    }

    /**
     * Tests that the POST /{databases}/{table}/indexes/ endpoint properly
     * creates a index and that the
     * GET/{database}/{table}/index_status/{status_id} endpoint is working.
     */
    @Test
    public void postIndexAndCheckStatusTest() throws InterruptedException
    {
        Index testIndex = Fixtures.createTestIndexOneField();
        String tableStr = "{" + "\"fields\" : [\"" + testIndex.fields().get(0)
                + "\"]," + "\"name\" : \"" + testIndex.name() + "\"}";

        //act
        ResponseOptions response = given().body(tableStr).expect().statusCode(201)
                .body("index.name", equalTo(testIndex.name()))
                .body("index.fields", notNullValue())
                .body("index.createdAt", notNullValue())
                .body("index.updatedAt", notNullValue())
                .body("index.active", equalTo(false))//should not yet be active
                .body("id", notNullValue())
                .body("dateStarted", notNullValue())
                .body("statusLastUpdatedAt", notNullValue())
                .body("eta", notNullValue())
                .body("precentComplete", notNullValue())
                .body("totalRecords", equalTo(0))
                .body("statusLink", notNullValue())
                .body("recordsCompleted", equalTo(0))
                .when().post("/" + testIndex.name()).andReturn();

        Thread.sleep(100);//sleep for a hair to let the indexing complete

        String restAssuredBasePath = RestAssured.basePath;
        try
        {
            //get the uuid from the response
            String uuidString = response.getBody().jsonPath().get("id");
            RestAssured.basePath = "/" + testIndex.databaseName() + "/" + testIndex.tableName() + "/index_status/";
            ResponseOptions res = expect().statusCode(200)
                    .body("id", equalTo(uuidString))
                    .body("dateStarted", notNullValue())
                    .body("statusLastUpdatedAt", notNullValue())
                    .body("eta", notNullValue())
                    .body("precentComplete", notNullValue())
                    .body("index", notNullValue())
                    .body("index.active", equalTo(true))//should now be active
                    .body("totalRecords", notNullValue())
                    .body("statusLink", containsString(uuidString))
                    .body("recordsCompleted", notNullValue())
                    .when().get(uuidString).andReturn();//check status (index_status endpoint)
            LOGGER.debug("Status Response: " + res.getBody().prettyPrint());
        } finally
        {
            RestAssured.basePath = restAssuredBasePath;
        }
    }

    /**
     * Tests that the POST /{databases}/{table}/indexes/ endpoint properly
     * creates a index and that the
     * GET/{database}/{table}/index_status/{status_id} endpoint is working.
     */
    @Test
    public void createDataThenPostIndexAndCheckStatusTest() throws InterruptedException, Exception
    {
        String restAssuredBasePath = RestAssured.basePath;
        Database testDb = Fixtures.createTestPlayersDatabase();
        Table testTable = Fixtures.createTestPlayersTable();
        List<Document> docs = Fixtures.getBulkDocuments("./src/test/resources/players-short.json", testTable);
        try
        {
            //data insert          
            f.insertDatabase(testDb);
            f.insertTable(testTable);
            f.insertDocuments(docs);//put in a ton of data directly into the db
            Index lastname = Fixtures.createTestPlayersIndexLastName();
            String tableStr = "{" + "\"fields\" : [\"" + lastname.fields().get(0)
                    + "\"]," + "\"name\" : \"" + lastname.name() + "\"}";
            RestAssured.basePath = "/" + lastname.databaseName() + "/" + lastname.tableName() + "/indexes";
            //act -- create index
            ResponseOptions response = given().body(tableStr).expect().statusCode(201)
                    .body("index.name", equalTo(lastname.name()))
                    .body("index.fields", notNullValue())
                    .body("index.createdAt", notNullValue())
                    .body("index.updatedAt", notNullValue())
                    .body("index.active", equalTo(false))//should not yet be active
                    .body("id", notNullValue())
                    .body("dateStarted", notNullValue())
                    .body("statusLastUpdatedAt", notNullValue())
                    .body("eta", notNullValue())
                    .body("precentComplete", notNullValue())
                    .body("totalRecords", equalTo(3308))
                    .body("statusLink", notNullValue())
                    .body("recordsCompleted", equalTo(0))
                    .when().post("/" + lastname.name()).andReturn();

            //start a timer
            StopWatch sw = new StopWatch();
            sw.start();

            //check the status endpoint to make sure it got created
            //get the uuid from the response
            String uuidString = response.getBody().jsonPath().get("id");
            RestAssured.basePath = "/" + lastname.databaseName() + "/" + lastname.tableName() + "/index_status/";
            ResponseOptions res = expect().statusCode(200)
                    .body("id", equalTo(uuidString))
                    .body("dateStarted", notNullValue())
                    .body("statusLastUpdatedAt", notNullValue())
                    .body("eta", notNullValue())
                    .body("precentComplete", notNullValue())
                    .body("index", notNullValue())
                    .body("index.active", notNullValue())
                    .body("index.active", equalTo(false))//should not yet be active
                    .body("statusLink", containsString(uuidString))
                    .body("recordsCompleted", notNullValue())
                    .when().get(uuidString).andReturn();
            LOGGER.debug("Status Response: " + res.getBody().prettyPrint());

            boolean active = false;
            while (!active)
            {
                //poll the status until it is active to make sure an index did in fact get created
                res = expect().statusCode(200)
                        .body("id", equalTo(uuidString))
                        .body("dateStarted", notNullValue())
                        .body("statusLastUpdatedAt", notNullValue())
                        .body("eta", notNullValue())
                        .body("precentComplete", notNullValue())
                        .body("index", notNullValue())
                        .body("index.active", notNullValue())
                        .body("statusLink", containsString(uuidString))
                        .body("recordsCompleted", notNullValue())
                        .when().get(uuidString).andReturn();
                LOGGER.debug("Status Response: " + res.getBody().prettyPrint());
                active = res.getBody().jsonPath().get("index.active");
                if (active)
                {
                    sw.stop();
                    break;
                }
                LOGGER.debug("Waiting for index to go active for: " + sw.getTime());
                if (sw.getTime() == 12000)
                {
                    fail("Index took too long to create");
                }
                Thread.sleep(5000);
            }
            LOGGER.info("It took: " + (sw.getTime() / 1000) + " seconds to create the index.");

        } finally
        {
            RestAssured.basePath = restAssuredBasePath;
            //clean up
            DocumentRepository docrepo = new DocumentRepository(f.getSession());
            for(Document d : docs){
                docrepo.delete(d);
            }
        }
    }

    /**
     * Tests that the POST /{databases}/{table}/indexes/ endpoint properly
     * creates a index and that the GET/{database}/{table}/index_status/
     * endpoint is working.
     */
    @Test
    public void postIndexAndCheckStatusAllTest() throws Exception
    {
        String restAssuredBasePath = RestAssured.basePath;
        try
        {
            //data insert
            Database testDb = Fixtures.createTestPlayersDatabase();
            Table testTable = Fixtures.createTestPlayersTable();
            f.insertDatabase(testDb);
            f.insertTable(testTable);
            List<Document> docs = Fixtures.getBulkDocuments("./src/test/resources/players-short.json", testTable);
            f.insertDocuments(docs);//put in a ton of data directly into the db
            Index lastname = Fixtures.createTestPlayersIndexLastName();
            String tableStr = "{" + "\"fields\" : [\"" + lastname.fields().get(0)
                    + "\"]," + "\"name\" : \"" + lastname.name() + "\"}";
            RestAssured.basePath = "/" + lastname.databaseName() + "/" + lastname.tableName() + "/indexes";
            //act -- create index
            given().body(tableStr).expect().statusCode(201)
                    .body("index.name", equalTo(lastname.name()))
                    .body("index.fields", notNullValue())
                    .body("index.createdAt", notNullValue())
                    .body("index.updatedAt", notNullValue())
                    .body("index.active", equalTo(false))//should not yet be active
                    .body("id", notNullValue())
                    .body("dateStarted", notNullValue())
                    .body("statusLastUpdatedAt", notNullValue())
                    .body("eta", notNullValue())
                    .body("precentComplete", notNullValue())
                    .body("totalRecords", equalTo(3308))
                    .body("statusLink", notNullValue())
                    .body("recordsCompleted", equalTo(0))
                    .when().post("/" + lastname.name());

            //start a timer
            StopWatch sw = new StopWatch();
            sw.start();

            RestAssured.basePath = "/" + lastname.databaseName() + "/" + lastname.tableName() + "/index_status/";

            //check to make sure it shows as present at least once
            ResponseOptions res = expect().statusCode(200)
                    .body("[0].id", notNullValue())
                    .body("[0].dateStarted", notNullValue())
                    .body("[0].statusLastUpdatedAt", notNullValue())
                    .body("[0].eta", notNullValue())
                    .body("[0].index", notNullValue())
                    .body("[0].index.active", equalTo(false))//should not yet be active
                    .body("[0].totalRecords", notNullValue())
                    .body("[0].recordsCompleted", notNullValue())
                    .body("[0].precentComplete", notNullValue())
                    .body("[0].statusLink", notNullValue())
                    .when().get("/").andReturn();
            LOGGER.debug("Status Response: " + res.getBody().prettyPrint());
            //wait for it to dissapear (meaning it's gone active)
            boolean active = false;
            while (!active)
            {
                res = expect().statusCode(200).when().get("/").andReturn();
                String body = res.getBody().prettyPrint();
                LOGGER.debug("Status Response: " + body);

                JSONArray resultSet = (JSONArray) parser.parse(body);
                if (resultSet.isEmpty())
                {
                    active = true;
                    sw.stop();
                    break;
                }
                LOGGER.debug("Waiting for index to go active for: " + sw.getTime());
                if (sw.getTime() == 12000)
                {
                    fail("Index took too long to create");
                }
                Thread.sleep(5000);
            }
            LOGGER.info("It took: " + (sw.getTime() / 1000) + " seconds to create the index.");

        } finally
        {
            RestAssured.basePath = restAssuredBasePath;
        }
    }

    /**
     * Tests that the DELETE /{databases}/{table}/indexes/{index} endpoint
     * properly deletes a index.
     */
    @Test
    public void deleteIndexTest()
    {
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

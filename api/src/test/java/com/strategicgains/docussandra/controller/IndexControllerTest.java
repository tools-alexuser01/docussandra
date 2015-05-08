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
import com.strategicgains.docussandra.domain.IndexField;
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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
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
     * Tests that the GET /{databases}/{setTable}/indexes/{index} properly
     * retrieves an existing index.
     */
    @Test
    public void getIndexTest()
    {
        Index testIndex = Fixtures.createTestIndexOneField();
        f.insertIndex(testIndex);
        ResponseOptions result = expect().statusCode(200)
                .body("name", equalTo(testIndex.getName()))
                .body("fields", notNullValue())
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(testIndex.getName()).andReturn();

        LOGGER.info(result.body().prettyPrint());
    }

    /**
     * Tests that the POST /{databases}/{setTable}/indexes/ endpoint properly
     * creates a index.
     */
    @Test
    public void postIndexTest() throws InterruptedException
    {
        Index testIndex = Fixtures.createTestIndexOneField();
        String indexStr = "{" + "\"fields\" : [\"" + testIndex.getFieldsValues().get(0)
                + "\"], \"name\" : \"" + testIndex.getName() + "\"}";

        //act
        given().body(indexStr).expect().statusCode(201)
                .body("index.name", equalTo(testIndex.getName()))
                .body("index.fields", notNullValue())
                .body("index.createdAt", notNullValue())
                .body("index.updatedAt", notNullValue())
                .body("index.active", equalTo(false))
                .body("id", notNullValue())
                .body("dateStarted", notNullValue())
                .body("statusLastUpdatedAt", notNullValue())
                .body("totalRecords", equalTo(0))
                .body("recordsCompleted", equalTo(0))
                .when().post("/" + testIndex.getName());

        Thread.sleep(100);//sleep for a hair to let the indexing complete

        //check self (index endpoint)
        expect().statusCode(200)
                .body("name", equalTo(testIndex.getName()))
                .body("fields", notNullValue())
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .body("active", equalTo(true))
                .get("/" + testIndex.getName());
    }

    /**
     * Tests that the POST /{databases}/{setTable}/indexes/ endpoint properly
     * creates a index.
     */
    @Test
    public void postIndexAllFieldsTest() throws InterruptedException
    {
        Index testIndex = Fixtures.createTestIndexAllFieldTypes();
        String indexStr = Fixtures.generateIndexCreationStringWithFields(testIndex);

        //act
        given().body(indexStr).expect().statusCode(201)
                .body("index.name", equalTo(testIndex.getName()))
                .body("index.fields", notNullValue())
                .body("index.fields[0].field", equalTo(testIndex.getFields().get(0).getField()))
                .body("index.fields[0].type", equalTo(testIndex.getFields().get(0).getType().toString()))
                .body("index.fields[1].field", equalTo(testIndex.getFields().get(1).getField()))
                .body("index.fields[1].type", equalTo(testIndex.getFields().get(1).getType().toString()))
                .body("index.fields[2].field", equalTo(testIndex.getFields().get(2).getField()))
                .body("index.fields[2].type", equalTo(testIndex.getFields().get(2).getType().toString()))
                .body("index.fields[3].field", equalTo(testIndex.getFields().get(3).getField()))
                .body("index.fields[3].type", equalTo(testIndex.getFields().get(3).getType().toString()))
                .body("index.fields[4].field", equalTo(testIndex.getFields().get(4).getField()))
                .body("index.fields[4].type", equalTo(testIndex.getFields().get(4).getType().toString()))
                .body("index.fields[5].field", equalTo(testIndex.getFields().get(5).getField()))
                .body("index.fields[5].type", equalTo(testIndex.getFields().get(5).getType().toString()))
                .body("index.fields[6].field", equalTo(testIndex.getFields().get(6).getField()))
                .body("index.fields[6].type", equalTo(testIndex.getFields().get(6).getType().toString()))
                .body("index.createdAt", notNullValue())
                .body("index.updatedAt", notNullValue())
                .body("index.active", equalTo(false))
                .body("id", notNullValue())
                .body("dateStarted", notNullValue())
                .body("statusLastUpdatedAt", notNullValue())
                .body("totalRecords", equalTo(0))
                .body("recordsCompleted", equalTo(0))
                .when().log().ifValidationFails().post("/" + testIndex.getName());

        Thread.sleep(100);//sleep for a hair to let the indexing complete

        //check self (index endpoint)
        expect().statusCode(200)
                .body("name", equalTo(testIndex.getName()))
                .body("fields", notNullValue())
                .body("fields[0].field", equalTo(testIndex.getFields().get(0).getField()))
                .body("fields[0].type", equalTo(testIndex.getFields().get(0).getType().toString()))
                .body("fields[1].field", equalTo(testIndex.getFields().get(1).getField()))
                .body("fields[1].type", equalTo(testIndex.getFields().get(1).getType().toString()))
                .body("fields[2].field", equalTo(testIndex.getFields().get(2).getField()))
                .body("fields[2].type", equalTo(testIndex.getFields().get(2).getType().toString()))
                .body("fields[3].field", equalTo(testIndex.getFields().get(3).getField()))
                .body("fields[3].type", equalTo(testIndex.getFields().get(3).getType().toString()))
                .body("fields[4].field", equalTo(testIndex.getFields().get(4).getField()))
                .body("fields[4].type", equalTo(testIndex.getFields().get(4).getType().toString()))
                .body("fields[5].field", equalTo(testIndex.getFields().get(5).getField()))
                .body("fields[5].type", equalTo(testIndex.getFields().get(5).getType().toString()))
                .body("fields[6].field", equalTo(testIndex.getFields().get(6).getField()))
                .body("fields[6].type", equalTo(testIndex.getFields().get(6).getType().toString()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .body("active", equalTo(true))
                .get("/" + testIndex.getName());
    }

    /**
     * Tests that the POST /{databases}/{setTable}/indexes/ endpoint properly
     * creates a index and that the
     * GET/{database}/{setTable}/index_status/{status_id} endpoint is working.
     */
    @Test
    public void postIndexAndCheckStatusTest() throws InterruptedException
    {
        Index testIndex = Fixtures.createTestIndexOneField();
        String indexStr = "{" + "\"fields\" : [\"" + testIndex.getFieldsValues().get(0)
                + "\"]," + "\"name\" : \"" + testIndex.getName() + "\"}";

        //act
        ResponseOptions response = given().body(indexStr).expect().statusCode(201)
                .body("index.name", equalTo(testIndex.getName()))
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
                .body("recordsCompleted", equalTo(0))
                .when().post("/" + testIndex.getName()).andReturn();

        Thread.sleep(100);//sleep for a hair to let the indexing complete

        String restAssuredBasePath = RestAssured.basePath;
        try
        {
            //get the uuid from the response
            String uuidString = response.getBody().jsonPath().get("id");
            RestAssured.basePath = "/" + testIndex.getDatabaseName() + "/" + testIndex.getTableName() + "/index_status/";
            ResponseOptions res = expect().statusCode(200)
                    .body("id", equalTo(uuidString))
                    .body("dateStarted", notNullValue())
                    .body("statusLastUpdatedAt", notNullValue())
                    .body("eta", notNullValue())
                    .body("precentComplete", notNullValue())
                    .body("index", notNullValue())
                    .body("index.active", equalTo(true))//should now be active
                    .body("totalRecords", notNullValue())
                    .body("recordsCompleted", notNullValue())
                    .when().get(uuidString).andReturn();//check status (index_status endpoint)
            LOGGER.debug("Status Response: " + res.getBody().prettyPrint());
        } finally
        {
            RestAssured.basePath = restAssuredBasePath;
        }
    }

    /**
     * Tests that the POST /{databases}/{setTable}/indexes/ endpoint properly
     * creates a index and that the
     * GET/{database}/{setTable}/index_status/{status_id} endpoint is working.
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
            String indexStr = "{" + "\"fields\" : [\"" + lastname.getFieldsValues().get(0)
                    + "\"]," + "\"name\" : \"" + lastname.getName() + "\"}";
            RestAssured.basePath = "/" + lastname.getDatabaseName() + "/" + lastname.getTableName() + "/indexes";
            //act -- create index
            ResponseOptions response = given().body(indexStr).expect().statusCode(201)
                    .body("index.name", equalTo(lastname.getName()))
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
                    .body("recordsCompleted", equalTo(0))
                    .when().post("/" + lastname.getName()).andReturn();

            //start a timer
            StopWatch sw = new StopWatch();
            sw.start();

            //check the status endpoint to make sure it got created
            //get the uuid from the response
            String uuidString = response.getBody().jsonPath().get("id");
            RestAssured.basePath = "/" + lastname.getDatabaseName() + "/" + lastname.getTableName() + "/index_status/";
            ResponseOptions res = expect().statusCode(200)
                    .body("id", equalTo(uuidString))
                    .body("dateStarted", notNullValue())
                    .body("statusLastUpdatedAt", notNullValue())
                    .body("eta", notNullValue())
                    .body("precentComplete", notNullValue())
                    .body("index", notNullValue())
                    .body("index.active", notNullValue())
                    .body("index.active", equalTo(false))//should not yet be active
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
                if (sw.getTime() >= 60000)
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
            for (Document d : docs)
            {
                try
                {
                    docrepo.delete(d);
                } catch (Exception e)
                {
                    ;//eh -- the doc probably never got created
                }
            }
        }
    }

    /**
     * Tests that the POST /{databases}/{setTable}/indexes/ endpoint properly
     * creates a index, setting errors to the index status table and that the
     * GET/{database}/{setTable}/index_status/{status_id} endpoint is working.
     */
    @Test
    public void createBadDataThenPostIndexAndCheckStatusTest() throws InterruptedException, Exception
    {
        String restAssuredBasePath = RestAssured.basePath;
        Database testDb = Fixtures.createTestPlayersDatabase();
        Table testTable = Fixtures.createTestPlayersTable();
        List<Document> docs = Fixtures.getBulkDocuments("./src/test/resources/players-short.json", testTable);
        //botch a doc
        Document badDoc = docs.get(0);
        //the year field is now text
        badDoc.object("{\"SOURCE\":\"kffl\",\"LINK\":\"http://www.kffl.com/gnews.php?id=796662-eagles-michael-vick-showed-quick-release\",\"CREATEDON\":\"July, 07 2012 00:00:00\",\"ROOKIEYEAR\":\"TWO THOUSAND AND ONE\",\"NAMEFIRST\":\"Michael\",\"POSITION\":\"QB\",\"NAMEFULL\":\"Michael Vick\",\"TEAM\":\"PHI\",\"TITLE\":\"Michael Vick showed quick release\",\"NAMELAST\":\"Vick\"}");
        docs.set(0, badDoc);
        try
        {
            //data insert          
            f.insertDatabase(testDb);
            f.insertTable(testTable);
            f.insertDocuments(docs);//put in a ton of data directly into the db
            Index rookieyear = Fixtures.createTestPlayersIndexRookieYear();

            String indexString = Fixtures.generateIndexCreationStringWithFields(rookieyear);
            RestAssured.basePath = "/" + rookieyear.getDatabaseName() + "/" + rookieyear.getTableName() + "/indexes";
            //act -- create index
            ResponseOptions response = given().body(indexString).expect().statusCode(201)
                    .body("index.name", equalTo(rookieyear.getName()))
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
                    .body("recordsCompleted", equalTo(0))
                    .when().post("/" + rookieyear.getName()).andReturn();

            //start a timer
            StopWatch sw = new StopWatch();
            sw.start();

            //check the status endpoint to make sure it got created
            //get the uuid from the response
            String uuidString = response.getBody().jsonPath().get("id");
            RestAssured.basePath = "/" + rookieyear.getDatabaseName() + "/" + rookieyear.getTableName() + "/index_status/";
            ResponseOptions res = expect().statusCode(200)
                    .body("id", equalTo(uuidString))
                    .body("dateStarted", notNullValue())
                    .body("statusLastUpdatedAt", notNullValue())
                    .body("eta", notNullValue())
                    .body("precentComplete", notNullValue())
                    .body("index", notNullValue())
                    .body("index.active", notNullValue())
                    .body("index.active", equalTo(false))//should not yet be active
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
                if (sw.getTime() >= 60000)
                {
                    fail("Index took too long to create: " + sw.getTime());
                }
                Thread.sleep(5000);
            }
            LOGGER.info("It took: " + (sw.getTime() / 1000) + " seconds to create the index.");

            //once it is active, lets check and make sure we have an error in the status table for our bad doc -- side note: there are tons more errors than our intentional one in this dataset
            res = expect().statusCode(200)
                    .body("id", equalTo(uuidString))
                    .body("dateStarted", notNullValue())
                    .body("statusLastUpdatedAt", notNullValue())
                    .body("eta", notNullValue())
                    .body("precentComplete", notNullValue())
                    .body("index", notNullValue())
                    .body("index.active", notNullValue())
                    .body("recordsCompleted", notNullValue())
                    .when().get(uuidString).andReturn();
            LOGGER.debug("Status Response: " + res.getBody().prettyPrint());
            Assert.assertTrue(res.getBody().prettyPrint().contains("error"));
            Assert.assertTrue(res.getBody().prettyPrint().contains("796662-eagles-michael-vick-showed-quick-release"));

        } finally
        {
            RestAssured.basePath = restAssuredBasePath;
            //clean up
            DocumentRepository docrepo = new DocumentRepository(f.getSession());
            for (Document d : docs)
            {
                try
                {
                    docrepo.delete(d);
                } catch (Exception e)
                {
                    ;//eh -- the doc probably never got created
                }
            }
        }
    }

    /**
     * Tests that the POST /{databases}/{setTable}/indexes/ endpoint properly
     * creates a index and that the GET/{database}/{setTable}/index_status/
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
            String indexString = "{" + "\"fields\" : [\"" + lastname.getFieldsValues().get(0)
                    + "\"]," + "\"name\" : \"" + lastname.getName() + "\"}";
            RestAssured.basePath = "/" + lastname.getDatabaseName() + "/" + lastname.getTableName() + "/indexes";
            //act -- create index
            given().body(indexString).expect().statusCode(201)
                    .body("index.name", equalTo(lastname.getName()))
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
                    .body("recordsCompleted", equalTo(0))
                    .when().log().ifValidationFails().post("/" + lastname.getName());

            //start a timer
            StopWatch sw = new StopWatch();
            sw.start();

            RestAssured.basePath = "/index_status/";

            //check to make sure it shows as present at least once
            ResponseOptions res = expect().statusCode(200)
                    .body("_embedded.indexcreatedevents[0].id", notNullValue())
                    .body("_embedded.indexcreatedevents[0].dateStarted", notNullValue())
                    .body("_embedded.indexcreatedevents[0].statusLastUpdatedAt", notNullValue())
                    .body("_embedded.indexcreatedevents[0].eta", notNullValue())
                    .body("_embedded.indexcreatedevents[0].index", notNullValue())
                    .body("_embedded.indexcreatedevents[0].index.active", equalTo(false))//should not yet be active
                    .body("_embedded.indexcreatedevents[0].totalRecords", notNullValue())
                    .body("_embedded.indexcreatedevents[0].recordsCompleted", notNullValue())
                    .body("_embedded.indexcreatedevents[0].precentComplete", notNullValue())
                    .when().log().ifValidationFails().get("/").andReturn();
            LOGGER.debug("Status Response: " + res.getBody().prettyPrint());
            //wait for it to dissapear (meaning it's gone active)
            boolean active = false;
            while (!active)
            {
                res = expect().statusCode(200).when().get("/").andReturn();
                String body = res.getBody().prettyPrint();
                LOGGER.debug("Status Response: " + body);

                JSONObject bodyObject = (JSONObject) parser.parse(body);
                JSONObject embedded = (JSONObject) bodyObject.get("_embedded");
                JSONArray resultSet = (JSONArray) embedded.get("indexcreatedevents");
                if (resultSet.isEmpty())
                {
                    active = true;
                    sw.stop();
                    break;
                }
                LOGGER.debug("Waiting for index to go active for: " + sw.getTime());
                if (sw.getTime() >= 60000)
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
     * Tests that the DELETE /{databases}/{setTable}/indexes/{index} endpoint
     * properly deletes a index.
     */
    @Test
    public void deleteIndexTest()
    {
        Index testIndex = Fixtures.createTestIndexOneField();
        f.insertIndex(testIndex);
        //act
        given().expect().statusCode(204)
                .when().delete(testIndex.getName());
        //check
        expect().statusCode(404).when()
                .get(testIndex.getName());
    }

}

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
import static com.jayway.restassured.RestAssured.given;
import com.jayway.restassured.response.Response;
import com.strategicgains.docussandra.cache.CacheFactory;
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.persistence.DocumentRepository;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import static org.hamcrest.Matchers.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import testhelper.RestExpressManager;

/**
 *
 * @author udeyoje
 */
public class QueryControllerTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryControllerTest.class);
    private static final String BASE_URI = "http://localhost";
    private static final int PORT = 19080;
    private Fixtures f;

    private JSONParser parser = new JSONParser();

    public QueryControllerTest() throws Exception
    {
        f = Fixtures.getInstance(false);
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
        RestExpressManager.getManager().ensureRestExpressRunning(false);
    }

    @Before
    public void beforeTest() throws Exception
    {
        CacheFactory.clearAllCaches();//kill the cache and make it re-create for the purposes of this test.
        f.clearTestTables();
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        Table testTable = Fixtures.createTestTable();
        f.insertTable(testTable);
        f.insertIndex(Fixtures.createTestIndexOneField());
        f.insertIndex(Fixtures.createTestIndexTwoField());
        f.insertIndex(Fixtures.createTestIndexWithBulkDataHit());
        f.insertDocument(Fixtures.createTestDocument());
        Document onePrime = Fixtures.createTestDocument();
        onePrime.setUuid(new UUID(onePrime.getUuid().getMostSignificantBits() + 2, 1L));
        f.insertDocument(onePrime);
        f.insertDocument(Fixtures.createTestDocument2());
        Document twoPrime = Fixtures.createTestDocument2();
        twoPrime.setUuid(new UUID(twoPrime.getUuid().getMostSignificantBits() + 3, 2L));
        f.insertDocument(twoPrime);
        f.insertDocuments(Fixtures.getBulkDocuments());
        RestAssured.basePath = "/" + testDb.name() + "/" + testTable.name() + "/queries";
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
        f.clearTestTables();
    }

    /**
     * Tests that the POST /{databases}/{table}/query endpoint properly runs a
     * query.
     */
    @Test
    public void postQueryTest()
    {
        Query q = Fixtures.createTestQuery();
        //act
        given().body("{\"where\":\"" + q.getWhere() + "\"}").expect().statusCode(200)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("", notNullValue())
                .body("id", notNullValue())
                .body("id[0]", equalTo("00000000-0000-0000-0000-000000000001"))
                .body("object[0]", notNullValue())
                .body("object[0]", containsString("hello"))
                .when().post("");
    }

    /**
     * Tests that the POST /{databases}/{table}/query endpoint properly runs a
     * query with limits.
     */
    @Test
    public void postQueryTestOnNonIndexedField()
    {
        Query q = new Query();
        q.setWhere("field9999 = 'this is my data'");
        q.setTable("mytable");
        //act
        given().body("{\"where\":\"" + q.getWhere() + "\"}").expect().statusCode(206)
                .expect().statusCode(400)
                .body("", notNullValue())
                .body("error", containsString("field9999"))
                .when().post("");
    }

    /**
     * Tests that the POST /{databases}/{table}/query endpoint properly runs a
     * query with limits.
     */
    @Test
    public void postQueryTestWithLimit()
    {
        Query q = new Query();
        q.setWhere("field1 = 'this is my data'");
        q.setTable("mytable");
        //act
        given().header("limit", "1").header("offset", "0").body("{\"where\":\"" + q.getWhere() + "\"}").expect().statusCode(206)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("", notNullValue())
                .body("id", notNullValue())
                .body("id[0]", equalTo(new UUID(Long.MAX_VALUE - 33, 1l).toString()))
                .body("object[0]", notNullValue())
                .body("object[0]", containsString("this is some more random data32"))
                .when().post("");
    }

    /**
     * Tests that the POST /{databases}/{table}/query endpoint properly runs a
     * query with limits.
     */
    @Test
    public void postQueryTestWithLimitSameAsResponse()
    {
        Query q = new Query();
        q.setWhere("field1 = 'this is my data'");
        q.setTable("mytable");
        //act
        given().header("limit", "34").header("offset", "0").body("{\"where\":\"" + q.getWhere() + "\"}").expect().statusCode(200)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("", notNullValue())
                .body("id", notNullValue())
                .body("id[0]", equalTo(new UUID(Long.MAX_VALUE - 33, 1l).toString()))
                .body("object[0]", notNullValue())
                .body("object[0]", containsString("this is some more random data32"))
                .when().post("");
    }

    /**
     * Tests that the POST /{databases}/{table}/query endpoint properly runs a
     * query with limits.
     */
    @Test
    public void postQueryTestWithLimitMoreThanResponse()
    {
        Query q = new Query();
        q.setWhere("field1 = 'this is my data'");
        q.setTable("mytable");
        //act
        given().header("limit", "10000").header("offset", "0").body("{\"where\":\"" + q.getWhere() + "\"}").expect().statusCode(200)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("", notNullValue())
                .body("id", notNullValue())
                .body("id[0]", equalTo(new UUID(Long.MAX_VALUE - 33, 1l).toString()))
                .body("object[0]", notNullValue())
                .body("object[0]", containsString("this is some more random data32"))
                .when().post("");
    }

    /**
     * Tests that the POST /{databases}/{table}/query endpoint properly runs a
     * query with limits.
     */
    @Test
    public void postQueryTestWithLimitAndOffset()
    {
        Query q = new Query();
        q.setWhere("field1 = 'this is my data'");
        q.setTable("mytable");
        //act
        given().header("limit", "1").header("offset", "1").body("{\"where\":\"" + q.getWhere() + "\"}").expect().statusCode(206)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("", notNullValue())
                .body("id", notNullValue())
                .body("id[0]", equalTo(new UUID(Long.MAX_VALUE - 32, 1l).toString()))
                .body("object[0]", notNullValue())
                .body("object[0]", containsString("this is some more random data31"))
                .when().post("");
    }

    /**
     * Tests that the POST /{databases}/{table}/query endpoint properly runs a
     * query with limits.
     */
    @Test
    public void postQueryTestWithLimitAndOffset2()
    {
        Query q = new Query();
        q.setWhere("field1 = 'this is my data'");
        q.setTable("mytable");
        //act
        given().header("limit", "2").header("offset", "2").body("{\"where\":\"" + q.getWhere() + "\"}").expect().statusCode(206)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("", notNullValue())
                .body("id", notNullValue())
                .body("id[0]", equalTo(new UUID(Long.MAX_VALUE - 31, 1l).toString()))
                .body("object[0]", notNullValue())
                .body("object[0]", containsString("this is some more random data30"))
                .body("id[1]", equalTo(new UUID(Long.MAX_VALUE - 30, 1l).toString()))
                .body("object[1]", notNullValue())
                .body("object[1]", containsString("this is some more random data29"))
                .when().post("");
    }

    /**
     * Tests querying on integer types.
     *
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testQueryOnIntegerType() throws IOException, ParseException
    {
        String restAssuredBasePath = RestAssured.basePath;
        Database testDb = Fixtures.createTestPlayersDatabase();
        Table testTable = Fixtures.createTestPlayersTable();
        List<Document> docs = Fixtures.getBulkDocuments("./src/test/resources/players-short.json", testTable);
        try
        {
            //data setup
            RestAssured.basePath = "/" + testTable.databaseName() + "/" + testTable.name() + "/queries";
            f.insertDatabase(testDb);
            f.insertTable(testTable);
            //throw a few indexes in (including the one we are testing)
            f.insertIndex(Fixtures.createTestPlayersIndexRookieYear());
            f.insertIndex(Fixtures.createTestPlayersIndexCreatedOn());
            f.insertIndex(Fixtures.createTestPlayersIndexLastName());
            f.insertDocuments(docs);//put in a ton of data directly into the db
            //end setup

            //act
            Response response = given().body("{\"where\":\"ROOKIEYEAR = '2001'\"}").header("limit", "2000").expect().statusCode(200)
                    //.header("Location", startsWith(RestAssured.basePath + "/"))
                    .body("", notNullValue())
                    .body("id", notNullValue())
                    .body("id[0]", notNullValue())
                    .body("object[0]", notNullValue())
                    .body("object[0]", containsString("2001"))
                    .when().post("").andReturn();

            String body = response.getBody().prettyPrint();
            LOGGER.debug("Status Response: " + body);

            JSONArray bodyArray = (JSONArray) parser.parse(body);
            for (Object responseElement : bodyArray)
            {
                JSONObject responseElementJson = (JSONObject) responseElement;
                assertNotNull(responseElementJson);
                assertNotNull(responseElementJson.get("id"));
                String object = (String) responseElementJson.get("object");
                assertNotNull(object);
                assertTrue(object.contains("2001"));
            }
        } finally
        {
            //clean up
            RestAssured.basePath = restAssuredBasePath;

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
     * Tests querying on integer types, with a query that doesn't contain an
     * integer.
     *
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testQueryOnIntegerTypeBadFormat() throws IOException, ParseException
    {
        String restAssuredBasePath = RestAssured.basePath;
        Database testDb = Fixtures.createTestPlayersDatabase();
        Table testTable = Fixtures.createTestPlayersTable();
        List<Document> docs = Fixtures.getBulkDocuments("./src/test/resources/players-short.json", testTable);
        try
        {
            //data setup
            RestAssured.basePath = "/" + testTable.databaseName() + "/" + testTable.name() + "/queries";
            f.insertDatabase(testDb);
            f.insertTable(testTable);
            //throw a few indexes in (including the one we are testing)
            f.insertIndex(Fixtures.createTestPlayersIndexRookieYear());
            f.insertIndex(Fixtures.createTestPlayersIndexCreatedOn());
            f.insertIndex(Fixtures.createTestPlayersIndexLastName());
            f.insertDocuments(docs);//put in a ton of data directly into the db
            //end setup

            //act
            Response response = given().body("{\"where\":\"ROOKIEYEAR = 'Two Thousand and One'\"}").header("limit", "2000")
                    .expect().statusCode(400)
                    .body("", notNullValue())
                    .body("error", containsString("Two Thousand and One"))
                    .body("error", containsString("ROOKIEYEAR"))
                    .when().post("").andReturn();

            String body = response.getBody().prettyPrint();
            LOGGER.debug("Status Response: " + body);

        } finally
        {
            //clean up
            RestAssured.basePath = restAssuredBasePath;

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
     * Tests querying on date types.
     *
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testQueryOnDateType() throws IOException, ParseException
    {
        String restAssuredBasePath = RestAssured.basePath;
        Database testDb = Fixtures.createTestPlayersDatabase();
        Table testTable = Fixtures.createTestPlayersTable();
        List<Document> docs = Fixtures.getBulkDocuments("./src/test/resources/players-short.json", testTable);
        try
        {
            //data setup
            RestAssured.basePath = "/" + testTable.databaseName() + "/" + testTable.name() + "/queries";
            f.insertDatabase(testDb);
            f.insertTable(testTable);
            //throw a few indexes in (including the one we are testing)
            f.insertIndex(Fixtures.createTestPlayersIndexRookieYear());
            f.insertIndex(Fixtures.createTestPlayersIndexCreatedOn());
            f.insertIndex(Fixtures.createTestPlayersIndexLastName());
            f.insertDocuments(docs);//put in a ton of data directly into the db
            //end setup

            //act
            Response response = given().body("{\"where\":\"CREATEDON = 'July, 07 2012 00:00:00'\"}").header("limit", "2000").expect().statusCode(200)
                    //.header("Location", startsWith(RestAssured.basePath + "/"))
                    .body("", notNullValue())
                    .body("id", notNullValue())
                    .body("id[0]", notNullValue())
                    .body("object[0]", notNullValue())
                    .body("object[0]", containsString("July, 07 2012 00:00:00"))
                    .when().post("").andReturn();

            String body = response.getBody().prettyPrint();
            LOGGER.debug("Status Response: " + body);

            JSONArray bodyArray = (JSONArray) parser.parse(body);
            for (Object responseElement : bodyArray)
            {
                JSONObject responseElementJson = (JSONObject) responseElement;
                assertNotNull(responseElementJson);
                assertNotNull(responseElementJson.get("id"));
                String object = (String) responseElementJson.get("object");
                assertNotNull(object);
                assertTrue(object.contains("July, 07 2012 00:00:00"));
            }
        } finally
        {
            //clean up
            RestAssured.basePath = restAssuredBasePath;

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

}

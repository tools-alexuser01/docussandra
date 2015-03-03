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
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.domain.Table;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.util.UUID;
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
public class QueryControllerTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryControllerTest.class);
    private static final String BASE_URI = "http://localhost";
    private static final int PORT = 19080;
    private Fixtures f;

    public QueryControllerTest()
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
        f.clearTestTables();
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        Table testTable = Fixtures.createTestTable();
        f.insertTable(testTable);
        f.insertIndex(Fixtures.createTestIndexOneField());
        f.insertIndex(Fixtures.createTestIndexTwoField());
        f.insertIndex(Fixtures.createTestIndexWithBulkDataHit());
        f.insertDocument(Fixtures.createTestDocument());
        f.insertDocument(Fixtures.createTestDocument2());
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
    public void postTableTest()
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
    public void postTableTestWithLimit()
    {
        Query q = new Query();
        q.setWhere("field1 = 'this is my data'");
        q.setTable("mytable");
        //act
        given().header("limit", "1").header("offset", "0").body("{\"where\":\"" + q.getWhere() + "\"}").expect().statusCode(200)
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
    public void postTableTestWithLimitAndOffset()
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
    public void postTableTestWithLimitAndOffset2()
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

}

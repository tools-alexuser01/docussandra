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
package com.strategicgains.docussandra.controller.perf.remote;

import com.jayway.restassured.RestAssured;
import static com.jayway.restassured.RestAssured.expect;
import static com.jayway.restassured.RestAssured.given;
import com.jayway.restassured.response.Response;
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import testhelper.RestExpressManager;

/**
 *
 * @author udeyoje
 */
public class PlayersRemote
{

    private final Logger logger = LoggerFactory.getLogger(PlayersRemote.class);
    private static final String BASE_URI = "http://localhost";//"https://docussandra.stg-prsn.com";
    private static final int PORT = 19080;

    private Database playersDb;
    private Table playersTable;
    private List<Index> indexes = new ArrayList<>();

    public PlayersRemote() throws IOException
    {
        RestExpressManager.getManager().ensureRestExpressRunning();
        RestAssured.baseURI = BASE_URI;
        RestAssured.port = PORT;
        RestAssured.basePath = "/";
        RestAssured.useRelaxedHTTPSValidation();

        playersDb = new Database("players");
        playersDb.description("A database about players.");

        playersTable = new Table();
        playersTable.name("players_table");
        playersTable.description("A table about players.");

        Index player = new Index("player");
        player.isUnique(false);
        List<String> fields = new ArrayList<>(1);
        fields.add("NAMEFULL");
        player.fields(fields);
        player.table(playersTable);

        Index lastname = new Index("lastname");
        lastname.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("NAMELAST");
        lastname.fields(fields);
        lastname.table(playersTable);

        Index lastAndFirst = new Index("lastandfirst");
        lastAndFirst.isUnique(false);
        fields = new ArrayList<>(2);
        fields.add("NAMELAST");
        fields.add("NAMEFIRST");
        lastAndFirst.fields(fields);
        lastAndFirst.table(playersTable);

        Index team = new Index("team");
        team.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("TEAM");
        team.fields(fields);
        team.table(playersTable);

        Index position = new Index("postion");
        position.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("POSITION");
        position.fields(fields);
        position.table(playersTable);

        Index rookie = new Index("rookieyear");
        rookie.isUnique(false);
        fields = new ArrayList<>(1);
        fields.add("ROOKIEYEAR");
        rookie.fields(fields);
        rookie.table(playersTable);

        indexes.add(team);
        indexes.add(position);
        indexes.add(player);
        indexes.add(rookie);
        indexes.add(lastAndFirst);
        indexes.add(lastname);

    }

    @Test
    @Ignore
    public void loadData() throws IOException, ParseException
    {
        List<Document> docs = Fixtures.getBulkDocuments("./src/test/resources/players.json", playersTable);
        //TODO: multi thread
        for (Document d : docs)
        {
            postDocument(d);
        }
    }

    @Before
    public void beforeTest() throws Exception
    {
        deleteDb();//should delete everything related to this table
        postDB();
        postTable();
        for (Index i : indexes)
        {
            postIndex(i);
        }
    }

    @After
    public void afterTest() throws InterruptedException
    {
        deleteDb();
        
    }

    private void postDB()
    {
        logger.debug("Creating test DB");
        String dbStr = "{" + "\"description\" : \"" + playersDb.description()
                + "\"," + "\"name\" : \"" + playersDb.name() + "\"}";
        //act
        given().body(dbStr).expect().statusCode(201)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("name", equalTo(playersDb.name()))
                .body("description", equalTo(playersDb.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post(playersDb.name());
        //check
        expect().statusCode(200)
                .body("name", equalTo(playersDb.name()))
                .body("description", equalTo(playersDb.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when()
                .get("/" + playersDb.getId());
    }

    private void deleteDb() throws InterruptedException
    {
        logger.debug("Deleteing test DB");
        //act
        given().when().delete(playersDb.name());
        Thread.sleep(10000);
    }

    private void postTable()
    {
        logger.debug("Creating test table");
        String tableStr = "{" + "\"description\" : \"" + playersTable.description()
                + "\"," + "\"name\" : \"" + playersTable.name() + "\"}";
        //act
        given().body(tableStr).expect().statusCode(201)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("name", equalTo(playersTable.name()))
                .body("description", equalTo(playersTable.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post(playersDb.name() + "/" + playersTable.name());
        //check
        expect().statusCode(200)
                .body("name", equalTo(playersTable.name()))
                .body("description", equalTo(playersTable.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(playersDb.name() + "/" + playersTable.name());
    }

//    private void deleteTable()
//    {
//        logger.debug("Deleteing test table");
//        given().expect().statusCode(204)
//                .when().delete(playersTable.name());
//        //check
//        expect().statusCode(404).when()
//                .get(playersTable.name());
//    }
    private void postIndex(Index index)
    {
        boolean first = true;
        StringBuilder tableStr = new StringBuilder("{" + "\"fields\" : [");
        for (String field : index.fields())
        {
            if (!first)
            {
                tableStr.append(", ");
            } else
            {
                first = false;
            }
            tableStr.append("\"");
            tableStr.append(field);
            tableStr.append("\"");
        }
        tableStr.append("],").append("\"name\" : \"").append(index.name()).append("\"}");

        //act
        given().body(tableStr).expect().statusCode(201)
                .body("name", equalTo(index.name()))
                .body("fields", notNullValue())
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post(playersDb.name() + "/" + playersTable.name() + "/indexes/" + index.name());

        //check
        expect().statusCode(200)
                .body("name", equalTo(index.name()))
                .body("fields", notNullValue())
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .get(playersDb.name() + "/" + playersTable.name() + "/indexes/" + index.name());
    }

    private void postDocument(Document d)
    {
        //act
        Response r = given().body(d.object()).expect().statusCode(201)
                .body("id", notNullValue())
                .body("object", notNullValue())
                .body("object", containsString("greeting"))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post("/").andReturn();
    }

}

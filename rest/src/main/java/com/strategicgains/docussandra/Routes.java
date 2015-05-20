package com.strategicgains.docussandra;

import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;

import org.restexpress.RestExpress;

import com.strategicgains.docussandra.config.Configuration;

public abstract class Routes
{

    public static void define(Configuration config, RestExpress server)
    {
        //health check        
        server.uri("/health.{format}", config.getHealthController())
                .action("getHealth", GET)
                .name(Constants.Routes.HEALTH).noSerialization();
        //build info via GET
        server.uri("/buildInfo.{format}", config.getBuildInfoController())
                .action("getBuildInfo", GET)
                .name(Constants.Routes.BUILD_INFO).noSerialization();

        //note: the /index_status route is for ALL index status, not isolated to a specific dataset;
        //this is intentional, it will allow ALL teams to see what ongoing operations that are occuring
        //to better understand the current load on the system and help with estimating completion of tasks
        server.uri("/index_status/", config.getIndexStatusController())
                .action("readAll", GET)
                .name(Constants.Routes.INDEX_STATUS_ALL);

        server.uri("/index_status", config.getIndexStatusController())
                .action("readAll", GET)
                .name(Constants.Routes.INDEX_STATUS_ALL);

        server.uri("/", config.getDatabaseController())
                .action("readAll", GET)
                .method(OPTIONS)
                .name(Constants.Routes.DATABASES);

        server.uri("/{database}", config.getDatabaseController())
                .method(GET, DELETE, PUT, POST)
                .method(OPTIONS)
                .name(Constants.Routes.DATABASE);

        server.uri("/{database}/", config.getTableController())
                .action("readAll", GET)
                .name(Constants.Routes.TABLES);

        server.uri("/{database}/{table}", config.getTableController())
                .method(GET, DELETE, PUT, POST)
                .name(Constants.Routes.TABLE);

        server.uri("/{database}/{table}/", config.getDocumentController())
                //			.action("readAll", GET)
                .method(POST)
                .name(Constants.Routes.DOCUMENTS);

        server.uri("/{database}/{table}/indexes", config.getIndexController())
                .action("readAll", GET)
                .name(Constants.Routes.INDEXES);

        server.uri("/{database}/{table}/indexes/{index}", config.getIndexController())
                .method(GET, DELETE, POST)
                .name(Constants.Routes.INDEX);

        server.uri("/{database}/{table}/index_status/{status_id}", config.getIndexStatusController())
                .method(GET)
                .name(Constants.Routes.INDEX_STATUS);

//        server.uri("/{database}/{table}/index_status/", config.getIndexStatusController())
//                .action("readAll", GET)
//                .name(Constants.Routes.INDEX_STATUS_ALL);//this route isn't supported; we don't have the level of info needed presently
//
//        server.uri("/{database}/{table}/index_status", config.getIndexStatusController())
//                .action("readAll", GET)
//                .name(Constants.Routes.INDEX_STATUS_ALL);//this route isn't supported we don't have the level of info needed presently
        server.uri("/{database}/{table}/{documentId}", config.getDocumentController())
                .method(GET, PUT, DELETE)
                .name(Constants.Routes.DOCUMENT);

        //TODO: Support /{database}/{table}/{key1}/{key2}/... style reads for multi-part keys
//		server.regex("///(*)", config.getDocumentsController())
        server.uri("/{database}/{table}/queries", config.getQueryController())
                .action("query", POST)
                .name(Constants.Routes.QUERY);
//
//		server.uri("/queries/{queryId}.{format}", config.getQueryController())
//			.action("executeSavedQuery", POST)
//			.method(GET, PUT, DELETE)
//			.name(Constants.Routes.QUERY);

//		server.uri("/samples/uuid/{uuid}.{format}", config.getSampleUuidEntityController())
//		    .method(GET, PUT, DELETE)
//		    .name(Constants.Routes.SINGLE_UUID_SAMPLE);
//	
//		server.uri("/samples/uuid.{format}", config.getSampleUuidEntityController())
//		    .method(POST)
//		    .name(Constants.Routes.SAMPLE_UUID_COLLECTION);
//	
//		server.uri("/samples/compound/{key1}/{key2}/{key3}.{format}", config.getSampleCompoundIdentifierEntityController())
//		    .method(GET, PUT, DELETE)
//		    .name(Constants.Routes.SINGLE_COMPOUND_SAMPLE);
//	
//		server.uri("/samples/compound/{key1}/{key2}.{format}", config.getSampleCompoundIdentifierEntityController())
//		    .action("readAll", GET)
//		    .method(POST)
//		    .name(Constants.Routes.SAMPLE_COMPOUND_COLLECTION);
    }
}

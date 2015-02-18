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
			.method(GET, PUT, DELETE, POST)
			.name(Constants.Routes.INDEX);

		server.uri("/{database}/{table}/{documentId}", config.getDocumentController())
			.method(GET, PUT, DELETE)
			.name(Constants.Routes.DOCUMENT);

		//TODO: Support /{database}/{table}/{key1}/{key2}/... style reads for multi-part keys
//		server.regex("///(*)", config.getDocumentsController())

//		server.uri("/{database}/{table}/queries", config.getQueryController())
//			.action("query", POST)
//			.name(Constants.Routes.QUERY);
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

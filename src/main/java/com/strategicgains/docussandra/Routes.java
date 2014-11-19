package com.strategicgains.docussandra;

import static org.jboss.netty.handler.codec.http.HttpMethod.*;

import org.restexpress.RestExpress;

import com.strategicgains.docussandra.config.Configuration;

public abstract class Routes
{
	public static void define(Configuration config, RestExpress server)
	{
		server.uri("/", config.getNamespacesController())
			.action("readAll", GET)
			.method(OPTIONS)
			.name(Constants.Routes.NAMESPACES);

		server.uri("/{namespace}", config.getNamespacesController())
			.method(GET, DELETE, PUT, POST)
			.method(OPTIONS)
			.name(Constants.Routes.NAMESPACE);

		server.uri("/{namespace}/", config.getCollectionsController())
			.action("readAll", GET)
			.name(Constants.Routes.COLLECTIONS);

		server.uri("/{namespace}/{collection}", config.getCollectionsController())
			.method(GET, DELETE, PUT, POST)
			.name(Constants.Routes.COLLECTION);

		server.uri("/{namespace}/{collection}/", config.getDocumentsController())
			.action("readAll", GET)
			.method(POST)
			.name(Constants.Routes.DOCUMENTS);

		server.uri("/{namespace}/{collection}/indexes", config.getIndexesController())
			.action("readAll", GET)
//			.method(PUT)
			.name(Constants.Routes.INDEXES);

		server.uri("/{namespace}/{collection}/indexes/{index}", config.getIndexesController())
			.method(GET, PUT, DELETE)
			.name(Constants.Routes.INDEX);

		server.uri("/{namespace}/{collection}/{documentId}", config.getDocumentsController())
			.method(GET, PUT, DELETE)
			.name(Constants.Routes.DOCUMENT);

//		server.uri("/queries.{format}", config.getQueryController())
//			.action("query", POST)
//			.name(Constants.Routes.QUERIES);
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

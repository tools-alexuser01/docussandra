package com.strategicgains.mongossandra;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.restexpress.RestExpress;
import com.strategicgains.mongossandra.config.Configuration;

public abstract class Routes
{
	public static void define(Configuration config, RestExpress server)
	{
		server.uri("/namespaces.{format}", config.getNamespacesController())
			.action("readAll", HttpMethod.GET)
			.method(HttpMethod.POST)
			.name(Constants.Routes.NAMESPACES);

		server.uri("/namespaces/{namespaceId}.{format}", config.getNamespacesController())
			.method(HttpMethod.GET, HttpMethod.PUT, HttpMethod.DELETE)
			.name(Constants.Routes.NAMESPACE);

		server.uri("/collections.{format}", config.getCollectionsController())
			.action("readAll", HttpMethod.GET)
			.method(HttpMethod.POST)
			.name(Constants.Routes.COLLECTIONS);

		server.uri("/collections/{collectionId}.{format}", config.getCollectionsController())
			.method(HttpMethod.GET, HttpMethod.PUT, HttpMethod.DELETE)
			.name(Constants.Routes.COLLECTION);

		server.uri("/collections/{collectionId}/entities.{format}", config.getEntitiesController())
			.action("readAll", HttpMethod.GET)
			.method(HttpMethod.POST)
			.name(Constants.Routes.ENTITIES);

		server.uri("/collections/{collectionId}/entities/{entityId}.{format}", config.getEntitiesController())
			.method(HttpMethod.GET, HttpMethod.PUT, HttpMethod.DELETE)
			.name(Constants.Routes.ENTITY);

		server.uri("/collections/{collectionId}/indexes.{format}", config.getIndexesController())
			.action("readAll", HttpMethod.GET)
			.method(HttpMethod.PUT)
			.name(Constants.Routes.INDEXES);

		server.uri("/collections/{collectionId}/indexes/{indexId}.{format}", config.getIndexesController())
			.method(HttpMethod.GET, HttpMethod.PUT, HttpMethod.DELETE)
			.name(Constants.Routes.INDEX);

		server.uri("/queries.{format}", config.getQueryController())
			.action("query", HttpMethod.POST)
			.name(Constants.Routes.QUERIES);

		server.uri("/queries/{queryId}.{format}", config.getQueryController())
			.action("querySaved", HttpMethod.POST)
			.method(HttpMethod.GET, HttpMethod.PUT, HttpMethod.DELETE)
			.name(Constants.Routes.QUERY);

//		server.uri("/samples/uuid/{uuid}.{format}", config.getSampleUuidEntityController())
//		    .method(HttpMethod.GET, HttpMethod.PUT, HttpMethod.DELETE)
//		    .name(Constants.Routes.SINGLE_UUID_SAMPLE);
//	
//		server.uri("/samples/uuid.{format}", config.getSampleUuidEntityController())
//		    .method(HttpMethod.POST)
//		    .name(Constants.Routes.SAMPLE_UUID_COLLECTION);
//	
//		server.uri("/samples/compound/{key1}/{key2}/{key3}.{format}", config.getSampleCompoundIdentifierEntityController())
//		    .method(HttpMethod.GET, HttpMethod.PUT, HttpMethod.DELETE)
//		    .name(Constants.Routes.SINGLE_COMPOUND_SAMPLE);
//	
//		server.uri("/samples/compound/{key1}/{key2}.{format}", config.getSampleCompoundIdentifierEntityController())
//		    .action("readAll", HttpMethod.GET)
//		    .method(HttpMethod.POST)
//		    .name(Constants.Routes.SAMPLE_COMPOUND_COLLECTION);
	}
}

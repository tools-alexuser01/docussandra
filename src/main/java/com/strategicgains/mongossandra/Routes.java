package com.strategicgains.mongossandra;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.restexpress.RestExpress;
import com.strategicgains.mongossandra.config.Configuration;

public abstract class Routes
{
	public static void define(Configuration config, RestExpress server)
	{
		server.uri("/databases.{format}", config.getNamespacesController())
			.alias("/dbs.{format}")
			.action("readAll", HttpMethod.GET)
			.method(HttpMethod.POST)
			.name(Constants.Routes.DATABASES);

		server.uri("/databases/{databaseId}.{format}", config.getNamespacesController())
			.alias("/dbs/{databaseId}.{format}")
			.method(HttpMethod.GET, HttpMethod.PUT, HttpMethod.DELETE)
			.name(Constants.Routes.DATABASE);

		server.uri("/tables.{format}", config.getCollectionsController())
			.action("readAll", HttpMethod.GET)
			.method(HttpMethod.POST)
			.name(Constants.Routes.TABLES);

		server.uri("/tables/{tableId}.{format}", config.getCollectionsController())
			.method(HttpMethod.GET, HttpMethod.PUT, HttpMethod.DELETE)
			.name(Constants.Routes.TABLE);

		server.uri("/tables/{tableId}/entities.{format}", config.getEntitiesController())
			.action("readAll", HttpMethod.GET)
			.method(HttpMethod.POST)
			.name(Constants.Routes.ENTITIES);

		server.uri("/tables/{tableId}/entities/{entityId}.{format}", config.getEntitiesController())
			.method(HttpMethod.GET, HttpMethod.PUT, HttpMethod.DELETE)
			.name(Constants.Routes.ENTITY);

		server.uri("/tables/{tableId}/indexes.{format}", config.getIndexesController())
			.action("readAll", HttpMethod.GET)
			.method(HttpMethod.PUT)
			.name(Constants.Routes.INDEXES);

		server.uri("/tables/{tableId}/indexes/{indexId}.{format}", config.getIndexesController())
			.method(HttpMethod.GET, HttpMethod.PUT, HttpMethod.DELETE)
			.name(Constants.Routes.INDEX);

		server.uri("/queries.{format}", config.getQueryController())
			.action("query", HttpMethod.POST)
			.name(Constants.Routes.QUERIES);

		server.uri("/queries/{queryId}.{format}", config.getQueryController())
			.action("executeSavedQuery", HttpMethod.POST)
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

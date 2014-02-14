package com.strategicgains.mongossandra.controller;

import java.util.Collections;
import java.util.List;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.restexpress.Request;
import org.restexpress.Response;

import com.strategicgains.hyperexpress.UrlBuilder;
import com.strategicgains.mongossandra.Constants;
import com.strategicgains.mongossandra.domain.Collection;
import com.strategicgains.mongossandra.domain.Namespace;
import com.strategicgains.mongossandra.service.CollectionsService;
import com.strategicgains.repoexpress.adapter.Identifiers;
import com.strategicgains.repoexpress.util.UuidConverter;

/**
 * This is the 'controller' layer, where HTTP details are converted to domain concepts and passed to the service layer.
 * Then service layer response information is enhanced with HTTP details, if applicable, for the response.
 * <p/>
 * This controller demonstrates how to process a Cassandra entity that is identified by a single, primary row key such
 * as a UUID.
 */
public class CollectionsController
{
	private CollectionsService service;
	
	public CollectionsController(CollectionsService collectionsService)
	{
		super();
		this.service = collectionsService;
	}

	public Collection create(Request request, Response response)
	{
		Collection entity = request.getBodyAs(Collection.class, "Resource details not provided");
		Collection saved = service.create(entity);

		// Construct the response for create...
		response.setResponseCreated();

		// Include the Location header...
		String locationPattern = request.getNamedUrl(HttpMethod.GET, Constants.Routes.COLLECTION);
		response.addLocationHeader(new UrlBuilder(locationPattern)
			.param(Constants.Url.COLLECTION_ID, Identifiers.UUID.format(saved.getId()))
			.build());

		// enrich the resource with links, etc. here...

		// Return the newly-created resource...
		return saved;
	}

	public Collection read(Request request, Response response)
	{
		String id = request.getHeader(Constants.Url.COLLECTION_ID, "No collection name supplied");
		Collection entity = service.read(id);

		// enrich the entity with links, etc. here...

		return entity;
	}

	public List<Namespace> readAll(Request request, Response response)
	{
//		return service.readAll();
		return Collections.emptyList();
	}

	public void update(Request request, Response response)
	{
		String id = request.getHeader(Constants.Url.COLLECTION_ID, "No resource ID supplied");
		Collection entity = request.getBodyAs(Collection.class, "Resource details not provided");

//		if (!Identifiers.UUID.parse(id).equals(entity.getId()))
//		{
//			throw new BadRequestException("ID in URL and ID in resource body must match");
//		}

		entity.setUuid(UuidConverter.parse(id));
		service.update(entity);
		response.setResponseNoContent();
	}

	public void delete(Request request, Response response)
	{
		String id = request.getHeader(Constants.Url.COLLECTION_ID, "No resource ID supplied");
		service.delete(id);
		response.setResponseNoContent();
	}
}

package com.strategicgains.mongossandra.controller;

import java.util.List;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.restexpress.Request;
import org.restexpress.Response;

import com.strategicgains.hyperexpress.UrlBuilder;
import com.strategicgains.mongossandra.Constants;
import com.strategicgains.mongossandra.domain.Collection;
import com.strategicgains.mongossandra.service.CollectionsService;
import com.strategicgains.repoexpress.domain.Identifier;

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
		String namespace = request.getHeader(Constants.Url.NAMESPACE, "No namespace provided");
		String name = request.getHeader(Constants.Url.COLLECTION, "No collection provided");
		Collection entity = request.getBodyAs(Collection.class, "Collection details not provided");
		entity.setNamespace(namespace);
		entity.setName(name);
		Collection saved = service.create(entity);

		// Construct the response for create...
		response.setResponseCreated();

		// Include the Location header...
		String locationPattern = request.getNamedUrl(HttpMethod.GET, Constants.Routes.COLLECTION);
		response.addLocationHeader(new UrlBuilder(locationPattern)
			.param(Constants.Url.COLLECTION, saved.getId().toString())
			.build());

		// enrich the resource with links, etc. here...

		// Return the newly-created resource...
		return saved;
	}

	public Collection read(Request request, Response response)
	{
		String namespace = request.getHeader(Constants.Url.NAMESPACE, "No namespace provided");
		String collection = request.getHeader(Constants.Url.COLLECTION, "No collection supplied");

		Collection entity = service.read(namespace, collection);

		// enrich the entity with links, etc. here...

		return entity;
	}

	public List<Collection> readAll(Request request, Response response)
	{
		String id = request.getHeader(Constants.Url.NAMESPACE, "No namespace provided");

		return service.readAll(id);
	}

	public void update(Request request, Response response)
	{
		String namespace = request.getHeader(Constants.Url.NAMESPACE, "No namespace provided");
		String name = request.getHeader(Constants.Url.COLLECTION, "No collection provided");
		Collection entity = request.getBodyAs(Collection.class, "Collection details not provided");
		entity.setNamespace(namespace);
		entity.setName(name);
		service.update(entity);
		response.setResponseNoContent();
	}

	public void delete(Request request, Response response)
	{
		String namespace = request.getHeader(Constants.Url.NAMESPACE, "No namespace provided");
		String collection = request.getHeader(Constants.Url.COLLECTION, "No collection provided");
		service.delete(new Identifier(namespace, collection));
		response.setResponseNoContent();
	}
}

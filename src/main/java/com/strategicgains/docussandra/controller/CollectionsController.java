package com.strategicgains.docussandra.controller;

import java.util.List;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.restexpress.Request;
import org.restexpress.Response;

import com.strategicgains.docussandra.Constants;
import com.strategicgains.docussandra.domain.Collection;
import com.strategicgains.docussandra.service.CollectionsService;
import com.strategicgains.hyperexpress.HyperExpress;
import com.strategicgains.hyperexpress.builder.TokenBinder;
import com.strategicgains.hyperexpress.builder.TokenResolver;
import com.strategicgains.hyperexpress.builder.UrlBuilder;
import com.strategicgains.repoexpress.domain.Identifier;

public class CollectionsController
{
	private static final UrlBuilder LOCATION_BUILDER = new UrlBuilder();

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
		Collection collection = request.getBodyAs(Collection.class);

		if (collection == null)
		{
			collection = new Collection();
		}

		collection.setNamespace(namespace);
		collection.setName(name);
		Collection saved = service.create(collection);

		// Construct the response for create...
		response.setResponseCreated();

		// enrich the resource with links, etc. here...
		TokenResolver resolver = HyperExpress.bind(Constants.Url.COLLECTION, saved.getId().components().get(1).toString());

		// Include the Location header...
		String locationPattern = request.getNamedUrl(HttpMethod.GET, Constants.Routes.COLLECTION);
		response.addLocationHeader(LOCATION_BUILDER.build(locationPattern, resolver));

		// Return the newly-created resource...
		return saved;
	}

	public Collection read(Request request, Response response)
	{
		String namespace = request.getHeader(Constants.Url.NAMESPACE, "No namespace provided");
		String collection = request.getHeader(Constants.Url.COLLECTION, "No collection supplied");

		Collection entity = service.read(namespace, collection);

		// enrich the entity with links, etc. here...
		HyperExpress.bind(Constants.Url.COLLECTION, entity.getId().components().get(1).toString());

		return entity;
	}

	public List<Collection> readAll(Request request, Response response)
	{
		String namespace = request.getHeader(Constants.Url.NAMESPACE, "No namespace provided");

		HyperExpress.tokenBinder(new TokenBinder<Collection>()
		{
			@Override
            public void bind(Collection object, TokenResolver resolver)
            {
				resolver.bind(Constants.Url.COLLECTION, object.getId().components().get(1).toString());
			}
		});
		return service.readAll(namespace);
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

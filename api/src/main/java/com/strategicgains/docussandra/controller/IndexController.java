package com.strategicgains.docussandra.controller;

import java.util.List;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.restexpress.Request;
import org.restexpress.Response;

import com.strategicgains.docussandra.Constants;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.service.IndexService;
import com.strategicgains.hyperexpress.HyperExpress;
import com.strategicgains.hyperexpress.builder.TokenBinder;
import com.strategicgains.hyperexpress.builder.TokenResolver;
import com.strategicgains.hyperexpress.builder.UrlBuilder;
import com.strategicgains.repoexpress.domain.Identifier;

/**
 * This is the 'controller' layer, where HTTP details are converted to domain concepts and passed to the service layer.
 * Then service layer response information is enhanced with HTTP details, if applicable, for the response.
 * <p/>
 * This controller demonstrates how to process a Cassandra entity that is identified by a single, primary row key such
 * as a UUID.
 */
public class IndexController
{
	private static final UrlBuilder LOCATION_BUILDER = new UrlBuilder();

	private IndexService indexes;
	
	public IndexController(IndexService indexService)
	{
		super();
		this.indexes = indexService;
	}

	public Index create(Request request, Response response)
	{
		String namespace = request.getHeader(Constants.Url.DATABASE, "No namespace provided");
		String collection = request.getHeader(Constants.Url.TABLE, "No collection provided");
		String name = request.getHeader(Constants.Url.INDEX, "No index name provided");
		Index entity = request.getBodyAs(Index.class, "Resource details not provided");
		entity.database(namespace);
		entity.table(collection);
		entity.name(name);
		Index saved = indexes.create(entity);

		// Construct the response for create...
		response.setResponseCreated();

		// enrich the resource with links, etc. here...
		TokenResolver resolver = HyperExpress.bind(Constants.Url.TABLE, saved.table())
			.bind(Constants.Url.DATABASE, saved.database())
			.bind(Constants.Url.INDEX, saved.name());

		// Include the Location header...
		String locationPattern = request.getNamedUrl(HttpMethod.GET, Constants.Routes.INDEX);
		response.addLocationHeader(LOCATION_BUILDER.build(locationPattern, resolver));

		// Return the newly-created resource...
		return saved;
	}

	public Index read(Request request, Response response)
	{
		String namespace = request.getHeader(Constants.Url.DATABASE, "No namespace provided");
		String collection = request.getHeader(Constants.Url.TABLE, "No collection provided");
		String name = request.getHeader(Constants.Url.INDEX, "No index name provided");
		Index entity = indexes.read(new Identifier(namespace, collection, name));

		// enrich the entity with links, etc. here...
		HyperExpress.bind(Constants.Url.TABLE, entity.table())
			.bind(Constants.Url.DATABASE, entity.database())
			.bind(Constants.Url.INDEX, entity.name());

		return entity;
	}

	public List<Index> readAll(Request request, Response response)
	{
		String namespace = request.getHeader(Constants.Url.DATABASE, "No namespace provided");
		String collection = request.getHeader(Constants.Url.TABLE, "No collection provided");

		HyperExpress.tokenBinder(new TokenBinder<Index>()
		{
			@Override
            public void bind(Index object, TokenResolver resolver)
            {
				resolver.bind(Constants.Url.TABLE, object.table())
					.bind(Constants.Url.DATABASE, object.database())
					.bind(Constants.Url.INDEX, object.name());

            }
		});

		return indexes.readAll(namespace, collection);
	}

	public void update(Request request, Response response)
	{
		String namespace = request.getHeader(Constants.Url.DATABASE, "No namespace provided");
		String collection = request.getHeader(Constants.Url.TABLE, "No collection provided");
		String name = request.getHeader(Constants.Url.INDEX, "No index name provided");
		Index entity = request.getBodyAs(Index.class, "Resource details not provided");
		entity.database(namespace);
		entity.table(collection);
		entity.name(name);
		indexes.update(entity);
		response.setResponseNoContent();
	}

	public void delete(Request request, Response response)
	{
		String namespace = request.getHeader(Constants.Url.DATABASE, "No namespace provided");
		String collection = request.getHeader(Constants.Url.TABLE, "No collection provided");
		String name = request.getHeader(Constants.Url.INDEX, "No index name provided");
		indexes.delete(new Identifier(namespace, collection, name));
		response.setResponseNoContent();
	}
}

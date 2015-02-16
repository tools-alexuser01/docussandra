package com.strategicgains.docussandra.controller;

import io.netty.handler.codec.http.HttpMethod;

import org.restexpress.Request;
import org.restexpress.Response;
import org.restexpress.exception.BadRequestException;

import com.strategicgains.docussandra.Constants;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.service.QueryService;
import com.strategicgains.hyperexpress.HyperExpress;
import com.strategicgains.hyperexpress.builder.TokenResolver;
import com.strategicgains.hyperexpress.builder.UrlBuilder;
import com.strategicgains.repoexpress.adapter.Identifiers;

/**
 * This is the 'controller' layer, where HTTP details are converted to domain concepts and passed to the service layer.
 * Then service layer response information is enhanced with HTTP details, if applicable, for the response.
 * <p/>
 * This controller demonstrates how to process a Cassandra entity that is identified by a single, primary row key such
 * as a UUID.
 */
public class QueryController
{
	private static final UrlBuilder LOCATION_BUILDER = new UrlBuilder();

	private QueryService service;
	
	public QueryController(QueryService queryService)
	{
		super();
		this.service = queryService;
	}

	public Query create(Request request, Response response)
	{
		Query entity = request.getBodyAs(Query.class, "Resource details not provided");
		Query saved = service.create(entity);

		// Construct the response for create...
		response.setResponseCreated();

		// enrich the resource with links, etc. here...
		TokenResolver resolver = HyperExpress.bind(Constants.Url.UUID, Identifiers.UUID.format(saved.getId()));

		// Include the Location header...
		String locationPattern = request.getNamedUrl(HttpMethod.GET, Constants.Routes.SINGLE_UUID_SAMPLE);
		response.addLocationHeader(LOCATION_BUILDER.build(locationPattern, resolver));

		// Return the newly-created resource...
		return saved;
	}

	public Query read(Request request, Response response)
	{
		String id = request.getHeader(Constants.Url.UUID, "No resource ID supplied");
		Query entity = service.read(Identifiers.UUID.parse(id));

		// enrich the entity with links, etc. here...

		return entity;
	}

	public void update(Request request, Response response)
	{
		String id = request.getHeader(Constants.Url.UUID, "No resource ID supplied");
		Query entity = request.getBodyAs(Query.class, "Resource details not provided");
		
		if (!Identifiers.UUID.parse(id).equals(entity.getId()))
		{
			throw new BadRequestException("ID in URL and ID in resource body must match");
		}

		service.update(entity);
		response.setResponseNoContent();
	}

	public void delete(Request request, Response response)
	{
		String id = request.getHeader(Constants.Url.UUID, "No resource ID supplied");
		service.delete(Identifiers.UUID.parse(id));
		response.setResponseNoContent();
	}
}

package com.strategicgains.mongossandra.controller;

import java.util.List;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.restexpress.Request;
import org.restexpress.Response;

import com.strategicgains.hyperexpress.UrlBuilder;
import com.strategicgains.mongossandra.Constants;
import com.strategicgains.mongossandra.domain.Namespace;
import com.strategicgains.mongossandra.service.NamespacesService;
import com.strategicgains.repoexpress.adapter.Identifiers;
import com.strategicgains.repoexpress.util.UuidConverter;

/**
 * This is the 'controller' layer, where HTTP details are converted to domain concepts and passed to the service layer.
 * Then service layer response information is enhanced with HTTP details, if applicable, for the response.
 * <p/>
 * This controller demonstrates how to process a Cassandra entity that is identified by a single, primary row key such
 * as a UUID.
 */
public class NamespacesController
{
	private NamespacesService service;
	
	public NamespacesController(NamespacesService sampleService)
	{
		super();
		this.service = sampleService;
	}

	public Namespace create(Request request, Response response)
	{
		Namespace entity = request.getBodyAs(Namespace.class, "Resource details not provided");
		Namespace saved = service.create(entity);

		// Construct the response for create...
		response.setResponseCreated();

		// Include the Location header...
		String locationPattern = request.getNamedUrl(HttpMethod.GET, Constants.Routes.NAMESPACE);
		response.addLocationHeader(new UrlBuilder(locationPattern)
			.param(Constants.Url.NAMESPACE_ID, Identifiers.UUID.format(saved.getId()))
			.build());

		// enrich the resource with links, etc. here...

		// Return the newly-created resource...
		return saved;
	}

	public Namespace read(Request request, Response response)
	{
		String id = request.getHeader(Constants.Url.NAMESPACE_ID, "No resource ID supplied");
		Namespace entity = service.read(id);

		// enrich the entity with links, etc. here...

		return entity;
	}
	
	public List<Namespace> readAll(Request request, Response response)
	{
		return service.readAll();
	}

	public void update(Request request, Response response)
	{
		String id = request.getHeader(Constants.Url.NAMESPACE_ID, "No resource ID supplied");
		Namespace entity = request.getBodyAs(Namespace.class, "Resource details not provided");

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
		String id = request.getHeader(Constants.Url.NAMESPACE_ID, "No resource ID supplied");
		service.delete(id);
		response.setResponseNoContent();
	}
}

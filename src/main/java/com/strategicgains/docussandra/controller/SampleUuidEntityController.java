package com.strategicgains.docussandra.controller;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.restexpress.Request;
import org.restexpress.Response;
import org.restexpress.exception.BadRequestException;

import com.strategicgains.docussandra.Constants;
import com.strategicgains.docussandra.domain.SampleUuidEntity;
import com.strategicgains.docussandra.service.SampleUuidEntityService;
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
public class SampleUuidEntityController
{
	private static final UrlBuilder LOCATION_BUILDER = new UrlBuilder();

	private SampleUuidEntityService service;
	
	public SampleUuidEntityController(SampleUuidEntityService sampleService)
	{
		super();
		this.service = sampleService;
	}

	public SampleUuidEntity create(Request request, Response response)
	{
		SampleUuidEntity entity = request.getBodyAs(SampleUuidEntity.class, "Resource details not provided");
		SampleUuidEntity saved = service.create(entity);

		// Construct the response for create...
		response.setResponseCreated();

		// enrich the resource with links, etc. here...
		TokenResolver resolver = HyperExpress.bind(Constants.Url.UUID, saved.getId().toString());

		// Include the Location header...
		String locationPattern = request.getNamedUrl(HttpMethod.GET, Constants.Routes.SINGLE_UUID_SAMPLE);
		response.addLocationHeader(LOCATION_BUILDER.build(locationPattern, resolver));

		// Return the newly-created resource...
		return saved;
	}

	public SampleUuidEntity read(Request request, Response response)
	{
		String id = request.getHeader(Constants.Url.UUID, "No resource ID supplied");
		SampleUuidEntity entity = service.read(Identifiers.UUID.parse(id));

		// enrich the entity with links, etc. here...

		return entity;
	}

	public void update(Request request, Response response)
	{
		String id = request.getHeader(Constants.Url.UUID, "No resource ID supplied");
		SampleUuidEntity entity = request.getBodyAs(SampleUuidEntity.class, "Resource details not provided");
		
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
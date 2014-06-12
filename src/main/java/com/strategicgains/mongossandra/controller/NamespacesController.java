package com.strategicgains.mongossandra.controller;

import java.util.List;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.restexpress.Request;
import org.restexpress.Response;

import com.strategicgains.hyperexpress.HyperExpress;
import com.strategicgains.hyperexpress.builder.TokenBinder;
import com.strategicgains.hyperexpress.builder.TokenResolver;
import com.strategicgains.hyperexpress.builder.UrlBuilder;
import com.strategicgains.mongossandra.Constants;
import com.strategicgains.mongossandra.domain.Namespace;
import com.strategicgains.mongossandra.service.NamespacesService;

/**
 * This is the 'controller' layer, where HTTP details are converted to domain concepts and passed to the service layer.
 * Then service layer response information is enhanced with HTTP details, if applicable, for the response.
 * <p/>
 * This controller demonstrates how to process a Cassandra entity that is identified by a single, primary row key such
 * as a UUID.
 */
public class NamespacesController
{
	private static final UrlBuilder LOCATION_BUILDER = new UrlBuilder();

	private NamespacesService service;
	
	public NamespacesController(NamespacesService sampleService)
	{
		super();
		this.service = sampleService;
	}

	public Namespace create(Request request, Response response)
	{
		String name = request.getHeader(Constants.Url.NAMESPACE, "No namespace name provided");
		Namespace entity = request.getBodyAs(Namespace.class, "Resource details not provided");
		entity.setName(name);
		Namespace saved = service.create(entity);

		// Construct the response for create...
		response.setResponseCreated();

		// enrich the resource with links, etc. here...
		TokenResolver resolver = HyperExpress.bind(Constants.Url.NAMESPACE, saved.getName());

		// Include the Location header...
		String locationPattern = request.getNamedUrl(HttpMethod.GET, Constants.Routes.NAMESPACE);
		response.addLocationHeader(LOCATION_BUILDER.build(locationPattern, resolver));

		// Return the newly-created resource...
		return saved;
	}

	public Namespace read(Request request, Response response)
	{
		String id = request.getHeader(Constants.Url.NAMESPACE, "No namespace provided");
		Namespace entity = service.read(id);

		// enrich the entity with links, etc. here...
		HyperExpress.bind(Constants.Url.NAMESPACE, entity.getName());

		return entity;
	}

	public List<Namespace> readAll(Request request, Response response)
	{
		HyperExpress.tokenBinder(new TokenBinder<Namespace>()
		{
			@Override
            public void bind(Namespace object, TokenResolver resolver)
            {
				resolver.bind(Constants.Url.NAMESPACE, object.getName());
            }
		});
		return service.readAll();
	}

	public void update(Request request, Response response)
	{
		String name = request.getHeader(Constants.Url.NAMESPACE, "No namespace provided");
		Namespace entity = request.getBodyAs(Namespace.class, "Resource details not provided");

		entity.setName(name);
		service.update(entity);
		response.setResponseNoContent();
	}

	public void delete(Request request, Response response)
	{
		String id = request.getHeader(Constants.Url.NAMESPACE, "No namespace name supplied");
		service.delete(id);
		response.setResponseNoContent();
	}
}

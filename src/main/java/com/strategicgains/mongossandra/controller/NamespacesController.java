package com.strategicgains.mongossandra.controller;

import static com.strategicgains.mongossandra.Constants.Routes.*;

import java.util.List;

import org.jboss.netty.handler.codec.http.HttpHeaders;
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
 * REST controller for Namespace entities.
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

	public void options(Request request, Response response)
	{
		if (NAMESPACES.equals(request.getResolvedRoute().getName()))
		{
			response.addHeader(HttpHeaders.Names.ALLOW, "GET");
		}
		else if (NAMESPACE.equals(request.getResolvedRoute().getName()))
		{
			response.addHeader(HttpHeaders.Names.ALLOW, "GET, DELETE, PUT, POST");			
		}
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

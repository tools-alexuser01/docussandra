package com.strategicgains.mongossandra.controller;

import java.nio.ByteBuffer;
import java.util.List;

import org.bson.BSON;
import org.bson.BSONObject;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.restexpress.Request;
import org.restexpress.Response;
import org.restexpress.exception.BadRequestException;

import com.strategicgains.hyperexpress.UrlBuilder;
import com.strategicgains.mongossandra.Constants;
import com.strategicgains.mongossandra.domain.Document;
import com.strategicgains.mongossandra.service.DocumentsService;
import com.strategicgains.repoexpress.adapter.Identifiers;
import com.strategicgains.repoexpress.util.UuidConverter;

public class DocumentsController
{
	private DocumentsService service;
	
	public DocumentsController(DocumentsService documentsService)
	{
		super();
		this.service = documentsService;
	}

	public Document create(Request request, Response response)
	{
		String namespace = request.getHeader(Constants.Url.NAMESPACE, "No namespace provided");
		String collection = request.getHeader(Constants.Url.COLLECTION, "No collection provided");
		byte[] data = request.getBody().array();

		if (data == null || data.length == 0) throw new BadRequestException("No document data provided");

		BSONObject bson = BSON.decode(data);
		Document saved = service.create(namespace, collection, data);

		// Construct the response for create...
		response.setResponseCreated();

		// Include the Location header...
		String locationPattern = request.getNamedUrl(HttpMethod.GET, Constants.Routes.SINGLE_UUID_SAMPLE);
		response.addLocationHeader(new UrlBuilder(locationPattern)
			.param(Constants.Url.UUID, Identifiers.UUID.format(saved.getId()))
			.build());

		// enrich the resource with links, etc. here...

		// Return the newly-created resource...
		return saved;
	}

	public Document read(Request request, Response response)
	{
		String namespace = request.getHeader(Constants.Url.NAMESPACE, "No namespace provided");
		String collection = request.getHeader(Constants.Url.COLLECTION, "No collection provided");
		String id = request.getHeader(Constants.Url.DOCUMENT_ID, "No document ID supplied");
		Document document = service.read(namespace, collection, Identifiers.UUID.parse(id));

		// enrich the entity with links, etc. here...

		return document;
	}

	public List<Document> readAll(Request request, Response response)
	{
		String namespace = request.getHeader(Constants.Url.NAMESPACE, "No namespace provided");
		String collection = request.getHeader(Constants.Url.COLLECTION, "No collection provided");

		return service.readAll(namespace, collection);
	}

	public void update(Request request, Response response)
	{
		String namespace = request.getHeader(Constants.Url.NAMESPACE, "No namespace provided");
		String collection = request.getHeader(Constants.Url.COLLECTION, "No collection provided");
		String id = request.getHeader(Constants.Url.DOCUMENT_ID, "No document ID supplied");
		byte[] data = request.getBody().array();

		if (data == null || data.length == 0) throw new BadRequestException("No document data provided");

		BSONObject bson = BSON.decode(data);
		Document document = new Document();
		document.setUuid(UuidConverter.parse(id));
		document.setNamespace(namespace);
		document.setCollection(collection);
		document.setObject(data);
		service.update(document);
		response.setResponseNoContent();
	}

	public void delete(Request request, Response response)
	{
		String namespace = request.getHeader(Constants.Url.NAMESPACE, "No namespace provided");
		String collection = request.getHeader(Constants.Url.COLLECTION, "No collection provided");
		String id = request.getHeader(Constants.Url.DOCUMENT_ID, "No resource ID supplied");
		service.delete(namespace, collection, Identifiers.UUID.parse(id));
		response.setResponseNoContent();
	}
}

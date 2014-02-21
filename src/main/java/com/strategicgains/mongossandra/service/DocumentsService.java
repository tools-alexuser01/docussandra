package com.strategicgains.mongossandra.service;

import java.nio.ByteBuffer;
import java.util.List;

import org.restexpress.exception.NotFoundException;

import com.strategicgains.mongossandra.domain.Document;
import com.strategicgains.mongossandra.persistence.CollectionsRepository;
import com.strategicgains.mongossandra.persistence.DocumentsRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.syntaxe.ValidationEngine;

public class DocumentsService
{
	private CollectionsRepository collections;
	private DocumentsRepository docs;
	
	public DocumentsService(CollectionsRepository collectionsRepository, DocumentsRepository documentsRepository)
	{
		super();
		this.docs = documentsRepository;
		this.collections = collectionsRepository;
	}

	public Document create(String namespace, String collection, ByteBuffer object)
	{
		verifyCollection(namespace, collection);
			
		Document doc = new Document();
		doc.setNamespace(namespace);
		doc.setCollection(collection);
		doc.setObject(object);
		ValidationEngine.validateAndThrow(doc);
		return docs.create(doc);
	}

	public Document read(String namespace, String collection, Identifier id)
    {
		verifyCollection(namespace, collection);
		return docs.read(id);
    }

	public List<Document> readAll(String namespace, String collection)
	{
		verifyCollection(namespace, collection);
		return docs.readAll(namespace, collection);
	}

	public long countAll(String namespace, String collection)
	{
		return docs.countAll(namespace, collection);
	}

	public void update(Document entity)
    {
		ValidationEngine.validateAndThrow(entity);
		docs.update(entity);
    }

	public void delete(String namespace, String collection, Identifier id)
    {
		verifyCollection(namespace, collection);
		docs.delete(id);
    }

	private void verifyCollection(String namespace, String collection)
    {
	    Identifier collectionId = new Identifier(namespace, collection);

		if (!collections.exists(collectionId))
		{
			throw new NotFoundException("Collection not found: " + collectionId.toString());
		}
    }
}

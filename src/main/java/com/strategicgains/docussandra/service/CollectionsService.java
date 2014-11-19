package com.strategicgains.docussandra.service;

import java.util.List;

import com.strategicgains.docussandra.domain.Collection;
import com.strategicgains.docussandra.persistence.CollectionsRepository;
import com.strategicgains.docussandra.persistence.NamespacesRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.exception.ItemNotFoundException;
import com.strategicgains.syntaxe.ValidationEngine;

public class CollectionsService
{
	private CollectionsRepository collections;
	private NamespacesRepository namespaces;
	
	public CollectionsService(NamespacesRepository namespaceRepository, CollectionsRepository collectionsRepository)
	{
		super();
		this.namespaces = namespaceRepository;
		this.collections = collectionsRepository;
	}

	public Collection create(Collection entity)
	{
		if (!namespaces.exists(new Identifier(entity.getNamespace())))
		{
			throw new ItemNotFoundException("Namespace not found: " + entity.getNamespace());
		}

		ValidationEngine.validateAndThrow(entity);
		return collections.create(entity);
	}

	public Collection read(String namespace, String collection)
	{
		Identifier id = new Identifier(namespace, collection);
		Collection c = collections.read(id);

		if (c == null) throw new ItemNotFoundException("Collection not found: " + id.toString());

		return c;
	}

	public List<Collection> readAll(String namespace)
	{
		if (!namespaces.exists(new Identifier(namespace))) throw new ItemNotFoundException("Namespace not found: " + namespace);

		return collections.readAll(namespace);
	}

	public void update(Collection entity)
    {
		ValidationEngine.validateAndThrow(entity);
		collections.update(entity);
    }

	public void delete(Identifier id)
    {
		collections.delete(id);
    }
}

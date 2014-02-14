package com.strategicgains.mongossandra.service;

import java.util.List;
import java.util.UUID;

import com.strategicgains.mongossandra.domain.Collection;
import com.strategicgains.mongossandra.domain.Namespace;
import com.strategicgains.mongossandra.persistence.CollectionsRepository;
import com.strategicgains.mongossandra.persistence.NamespacesRepository;
import com.strategicgains.repoexpress.adapter.Identifiers;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.exception.InvalidObjectIdException;
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
		// Translate namespace name to namespace ID before persisting.
		if (entity.getNamespace() != null)
		{
			Namespace n = namespaces.readByName(entity.getNamespace());
			
			if (n == null) throw new ItemNotFoundException("Namespace not found: " + entity.getNamespace());
			
			entity.setNamespaceId(n.getUuid());
		}

		ValidationEngine.validateAndThrow(entity);
		return collections.create(entity);
	}

	public Collection read(String name)
	{
		Collection n = collections.readByName(name);
		
		if (n == null)
		{
			Identifier id = null;
			
			try
			{
				id = Identifiers.UUID.parse(name);
			}
			catch (InvalidObjectIdException e)
			{
				throw new ItemNotFoundException("Collection not found: " + name);
			}

			n = collections.read(id);
		}
		
		if (n == null) throw new ItemNotFoundException("Collection not found: " + name);

		return n;
	}

	public List<Collection> readAll(UUID namespaceId)
	{
		return collections.readAll(namespaceId);
	}

	public void update(Collection entity)
    {
		ValidationEngine.validateAndThrow(entity);
		collections.update(entity);
    }

	public void delete(String name)
    {
		collections.delete(read(name));
    }
}

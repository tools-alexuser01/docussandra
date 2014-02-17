package com.strategicgains.mongossandra.service;

import java.util.List;
import java.util.UUID;

import com.strategicgains.mongossandra.domain.Collection;
import com.strategicgains.mongossandra.domain.Namespace;
import com.strategicgains.mongossandra.persistence.CollectionsRepository;
import com.strategicgains.mongossandra.persistence.NamespacesRepository;
import com.strategicgains.repoexpress.adapter.Identifiers;
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
		// Translate namespace name to namespace ID before persisting.
		if (entity.getNamespace() != null)
		{
			Identifier nId = null;
			
			try
			{
				nId = Identifiers.UUID.parse(entity.getNamespace());
			}
			catch(Exception e)
			{
				// Do nothing.
			}

			Namespace n = null;
			
			if (nId != null)
			{
				n = namespaces.read(nId);
			}
			else
			{
				n = namespaces.readByName(entity.getNamespace());
			}
			
			if (n == null) throw new ItemNotFoundException("Namespace not found: " + entity.getNamespace());
			
			entity.setNamespaceId(n.getUuid());
			entity.setNamespace(null);
		}

		ValidationEngine.validateAndThrow(entity);
		return collections.create(entity);
	}

	public Collection read(Identifier id)
	{
		Collection c = collections.read(new Identifier(id.components().get(1)));

		if (c == null) throw new ItemNotFoundException("Collection not found: " + id.toString());

		return c;
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

	public void delete(Identifier id)
    {
		collections.delete(read(id));
    }
}

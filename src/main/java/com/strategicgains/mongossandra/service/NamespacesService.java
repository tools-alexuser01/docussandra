package com.strategicgains.mongossandra.service;

import java.util.List;

import com.strategicgains.mongossandra.domain.Namespace;
import com.strategicgains.mongossandra.persistence.NamespacesRepository;
import com.strategicgains.repoexpress.adapter.Identifiers;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.exception.InvalidObjectIdException;
import com.strategicgains.repoexpress.exception.ItemNotFoundException;
import com.strategicgains.syntaxe.ValidationEngine;

public class NamespacesService
{
	private NamespacesRepository namespaces;
	
	public NamespacesService(NamespacesRepository samplesRepository)
	{
		super();
		this.namespaces = samplesRepository;
	}

	public Namespace create(Namespace entity)
	{
		ValidationEngine.validateAndThrow(entity);
		return namespaces.create(entity);
	}

//	public Namespace read(Identifier id)
//    {
//		return namespaces.read(id);
//    }

	public Namespace read(String name)
	{
		Namespace n = namespaces.readByName(name);
		
		if (n == null)
		{
			Identifier id = null;
			
			try
			{
				id = Identifiers.UUID.parse(name);
			}
			catch (InvalidObjectIdException e)
			{
				throw new ItemNotFoundException("Namespace not found: " + name);
			}

			n = namespaces.read(id);
		}
		
		if (n == null) throw new ItemNotFoundException("Namespace not found: " + name);

		return n;
	}

	public List<Namespace> readAll()
	{
		return namespaces.readAll();
	}

	public void update(Namespace entity)
    {
		ValidationEngine.validateAndThrow(entity);
		namespaces.update(entity);
    }

	public void delete(String name)
    {
		namespaces.delete(read(name));
    }
}

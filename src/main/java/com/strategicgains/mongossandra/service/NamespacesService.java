package com.strategicgains.mongossandra.service;

import java.util.List;

import com.strategicgains.mongossandra.domain.Namespace;
import com.strategicgains.mongossandra.persistence.NamespacesRepository;
import com.strategicgains.repoexpress.domain.Identifier;
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

	public Namespace read(Identifier id)
    {
		return namespaces.read(id);
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

	public void delete(Identifier id)
    {
		namespaces.delete(id);
    }
}

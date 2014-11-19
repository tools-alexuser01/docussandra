package com.strategicgains.docussandra.service;

import java.util.List;

import com.strategicgains.docussandra.domain.Namespace;
import com.strategicgains.docussandra.persistence.NamespacesRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.exception.ItemNotFoundException;
import com.strategicgains.syntaxe.ValidationEngine;

public class NamespacesService
{
	private NamespacesRepository namespaces;
	
	public NamespacesService(NamespacesRepository namespacesRepository)
	{
		super();
		this.namespaces = namespacesRepository;
	}

	public Namespace create(Namespace entity)
	{
		ValidationEngine.validateAndThrow(entity);
		return namespaces.create(entity);
	}

	public Namespace read(String name)
	{
		Namespace n = namespaces.read(new Identifier(name));
		
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
		namespaces.delete(new Identifier(name));
    }
}

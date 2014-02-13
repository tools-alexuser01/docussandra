package com.strategicgains.mongossandra.domain;

import com.strategicgains.repoexpress.domain.AbstractUuidEntity;

public class Collection
extends AbstractUuidEntity
{
	private String namespace;
	private String name;
	private String description;

	public Collection()
	{
		super();
	}

	public Collection(String name)
	{
		this();
		setName(name);
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public String getNamespace()
	{
		return namespace;
	}

	public void setNamespace(String namespace)
	{
		this.namespace = namespace;
	}

	public String getDescription()
	{
		return description;
	}

	public void setDescription(String description)
	{
		this.description = description;
	}
}

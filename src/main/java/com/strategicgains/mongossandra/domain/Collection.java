package com.strategicgains.mongossandra.domain;

import java.util.UUID;

import com.strategicgains.repoexpress.domain.AbstractUuidEntity;

public class Collection
extends AbstractUuidEntity
{
	private UUID namespaceId;
	private String name;

	public Collection()
	{
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
}

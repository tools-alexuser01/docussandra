package com.strategicgains.mongossandra.domain;

import com.strategicgains.repoexpress.domain.AbstractUuidEntity;

public class Collection
extends AbstractUuidEntity
{
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

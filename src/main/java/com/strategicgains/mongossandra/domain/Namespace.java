package com.strategicgains.mongossandra.domain;

import com.strategicgains.repoexpress.domain.AbstractUuidEntity;

public class Namespace
extends AbstractUuidEntity
{
	private String name;

	public Namespace()
	{
	}

	public Namespace(String name)
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

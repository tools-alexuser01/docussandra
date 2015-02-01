package com.strategicgains.docussandra.domain;

import com.strategicgains.repoexpress.domain.AbstractTimestampedIdentifiable;
import com.strategicgains.repoexpress.domain.Identifier;

public class Metadata
extends AbstractTimestampedIdentifiable
{
	private String id = "system";
	private String version;

	@Override
    public Identifier getId()
    {
		return (hasId() ? new Identifier(id, getUpdatedAt()) : null);
    }

	@Override
    public void setId(Identifier id)
    {
		// Do nothing.
    }

	public boolean hasId()
	{
		return (id != null);
	}

	public void id(String value)
	{
		this.id = value;
	}

	public String id()
	{
		return id;
	}

	public String version()
	{
		return version;
	}

	public void version(String version)
	{
		this.version = version;
	}
}

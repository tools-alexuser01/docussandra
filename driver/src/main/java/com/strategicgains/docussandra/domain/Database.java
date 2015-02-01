package com.strategicgains.docussandra.domain;

import com.strategicgains.docussandra.Constants;
import com.strategicgains.repoexpress.domain.AbstractTimestampedIdentifiable;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.syntaxe.annotation.RegexValidation;

public class Database
extends AbstractTimestampedIdentifiable
{
	@RegexValidation(name = "Namespace Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String name;
	private String description;

	//TODO: add consistency & distro metadata here.

	public Database()
	{
		super();
	}

	public Database(String name)
	{
		this();
		name(name);
	}

	public String name()
	{
		return name;
	}

	public void name(String name)
	{
		this.name = name;
	}

	@Override
	public Identifier getId()
	{
		return new Identifier(name);
	}

	@Override
	public void setId(Identifier id)
	{
		// do nothing.
	}

	public boolean hasDescription()
	{
		return (description != null);
	}

	public String description()
	{
		return description;
	}

	public void description(String description)
	{
		this.description = description;
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder(name);

		if (hasDescription())
		{
			sb.append(" (");
			sb.append(description);
			sb.append(")");
		}
		return sb.toString();
	}
}

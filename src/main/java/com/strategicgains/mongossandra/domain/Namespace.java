package com.strategicgains.mongossandra.domain;

import com.strategicgains.mongossandra.Constants;
import com.strategicgains.repoexpress.domain.AbstractTimestampedIdentifiable;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.syntaxe.annotation.RegexValidation;

public class Namespace
extends AbstractTimestampedIdentifiable
{
	@RegexValidation(name = "Namespace Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String name;
	private String description;

	public Namespace()
	{
		super();
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

	public String getDescription()
	{
		return description;
	}

	public void setDescription(String description)
	{
		this.description = description;
	}
}

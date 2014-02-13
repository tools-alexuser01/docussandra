package com.strategicgains.mongossandra.domain;

import com.strategicgains.mongossandra.Constants;
import com.strategicgains.repoexpress.domain.AbstractUuidEntity;
import com.strategicgains.syntaxe.annotation.RegexValidation;

public class Namespace
extends AbstractUuidEntity
{
	@RegexValidation(name = "Application Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String name;

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
}

package com.strategicgains.mongossandra.domain;

import com.strategicgains.mongossandra.Constants;
import com.strategicgains.repoexpress.domain.AbstractTimestampedIdentifiable;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import com.strategicgains.syntaxe.annotation.Required;

public class Collection
extends AbstractTimestampedIdentifiable
{
	@Required("Namespace")
	private String namespace;
	
	@RegexValidation(name = "Collection Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String name;
	private String description;

	public Collection()
	{
		super();
	}

	public String getNamespace()
	{
		return namespace;
	}

	public void setNamespace(String namespace)
	{
		this.namespace = namespace;
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public String getDescription()
	{
		return description;
	}

	public void setDescription(String description)
	{
		this.description = description;
	}

	@Override
    public Identifier getId()
    {
	    return new Identifier(namespace, name);
    }

	@Override
    public void setId(Identifier id)
    {
	    // do nothing.
    }
}

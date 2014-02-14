package com.strategicgains.mongossandra.domain;

import java.util.UUID;

import com.strategicgains.mongossandra.Constants;
import com.strategicgains.repoexpress.domain.AbstractUuidEntity;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import com.strategicgains.syntaxe.annotation.Required;

public class Collection
extends AbstractUuidEntity
{
	private String namespace;
	
	@Required("Namespace ID")
	private UUID namespaceId;
	
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

	public UUID getNamespaceId()
	{
		return namespaceId;
	}

	public void setNamespaceId(UUID namespaceId)
	{
		this.namespaceId = namespaceId;
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
}

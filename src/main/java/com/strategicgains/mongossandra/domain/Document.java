package com.strategicgains.mongossandra.domain;

import com.strategicgains.mongossandra.Constants;
import com.strategicgains.repoexpress.domain.AbstractUuidEntity;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import com.strategicgains.syntaxe.annotation.Required;

public class Document
extends AbstractUuidEntity
{
	@RegexValidation(name = "Namespace", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String namespace;

	@RegexValidation(name = "Collection", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String collection;

	private String object;

	public Document()
	{
	}

	public String getNamespace()
	{
		return namespace;
	}

	public void setNamespace(String namespace)
	{
		this.namespace = namespace;
	}

	public String getCollection()
	{
		return collection;
	}

	public void setCollection(String collection)
	{
		this.collection = collection;
	}

	public String getObject()
	{
		return object;
	}

	public void setObject(String object)
	{
		this.object = object;
	}
}

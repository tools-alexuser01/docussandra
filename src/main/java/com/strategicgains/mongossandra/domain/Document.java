package com.strategicgains.mongossandra.domain;

import java.nio.ByteBuffer;

import com.strategicgains.mongossandra.Constants;
import com.strategicgains.repoexpress.domain.AbstractUuidEntity;
import com.strategicgains.syntaxe.annotation.RegexValidation;

public class Document
extends AbstractUuidEntity
{
	@RegexValidation(name = "Namespace", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String namespace;

	@RegexValidation(name = "Collection", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String collection;

	private ByteBuffer object;

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

	public ByteBuffer getObject()
	{
		return object;
	}

	public void setObject(ByteBuffer object)
	{
		this.object = object;
	}
}

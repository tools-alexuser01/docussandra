package com.strategicgains.docussandra.domain;

import org.restexpress.plugin.hyperexpress.Linkable;

import com.strategicgains.docussandra.Constants;
import com.strategicgains.repoexpress.domain.AbstractUuidEntity;
import com.strategicgains.syntaxe.annotation.RegexValidation;

public class Document
extends AbstractUuidEntity
implements Linkable
{
	//TODO: use a composed ID that is serialized to a blob.
	//TODO: add any necessary metadata regarding a document.
	//TODO: documents are versioned per transaction.

	@RegexValidation(name = "Namespace", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String namespace;

	@RegexValidation(name = "Collection", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String collection;

	// The JSON document.
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

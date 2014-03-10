package com.strategicgains.mongossandra.domain;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.strategicgains.mongossandra.Constants;
import com.strategicgains.repoexpress.domain.AbstractTimestampedIdentifiable;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.syntaxe.annotation.RegexValidation;

public class Index
extends AbstractTimestampedIdentifiable
{
	@RegexValidation(name = "Namespace", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String namespace;

	@RegexValidation(name = "Collection", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String collection;

	@RegexValidation(name = "Index name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String name;

	private boolean isUnique = false;
	
	/**
	 * This is how many items will be stored in a single wide row, before creating another wide row.
	 */
	private long bucketSize = 2000l;
	private Map<String, IndexProperties> properties = new HashMap<String, Index.IndexProperties>();

	public Index()
	{
	}

	public Index(String name)
	{
		this();
		setName(name);
	}

	public String getName()
	{
		return name;
	}

	public Index setName(String name)
	{
		this.name = name;
		return this;
	}

	public boolean isUnique()
	{
		return isUnique;
	}

	public Index setUnique(boolean value)
	{
		this.isUnique = value;
		return this;
	}

	public Index addProperty(String name, String type)
	{
		addProperty(name, type, true);
		return this;
	}

	public Index addProperty(String name, String type, boolean isAscending)
	{
		properties.put(name, new IndexProperties(type, isAscending));
		return this;
	}

	public Set<String> getPropertyNames()
	{
		return properties.keySet();
	}

	@Override
    public Identifier getId()
    {
		return new Identifier(namespace, collection, name);
    }

	@Override
    public void setId(Identifier id)
    {
    }

	public class IndexProperties
	{
		private boolean isAscending = true;
		private String type;

		public IndexProperties(String type, boolean isAscending)
		{
			super();
			setType(type);
			setAscending(isAscending);
		}
		public boolean isAscending()
		{
			return isAscending;
		}

		public void setAscending(boolean isReversed)
		{
			this.isAscending = isReversed;
		}

		public String getType()
		{
			return type;
		}

		public void setType(String type)
		{
			this.type = type;
		}
	}
}

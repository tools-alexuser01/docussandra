package com.strategicgains.docussandra.domain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.restexpress.plugin.hyperexpress.Linkable;
import org.restexpress.util.Callback;

import com.strategicgains.docussandra.Constants;
import com.strategicgains.repoexpress.domain.AbstractTimestampedIdentifiable;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import com.strategicgains.syntaxe.annotation.Required;

public class Index
extends AbstractTimestampedIdentifiable
implements Linkable
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

	/**
	 * The list of fields, in order, that are being indexed. Prefixing a field with a dash ('-') means
	 * it's order in descending order.
	 */
	@Required("Fields")
	private List<String> fields;

	@Required("Index Type")
	private IndexType type;

	public Index()
	{
	}

	public Index(String name)
	{
		this();
		setName(name);
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

	public void setFields(List<String> props)
	{
		this.fields = new ArrayList<String>(props);
	}

	public List<String> getFields()
	{
		return (fields == null ? Collections.<String>emptyList() : Collections.unmodifiableList(fields));
	}

	@Override
    public Identifier getId()
    {
		return new Identifier(namespace, collection, name);
    }

	@Override
    public void setId(Identifier id)
    {
		// intentionally left blank.
    }

	public long getBucketSize()
	{
		return bucketSize;
	}

	public void setBucketSize(long bucketSize)
	{
		this.bucketSize = bucketSize;
	}

	public void iterateFields(Callback<IndexField> callback)
	{
		for (String field : fields)
		{
			callback.process(new IndexField(field));
		}
	}

	public class IndexField
	{
		private String field;
		private boolean isAscending = true;

		public IndexField(String value)
		{
			field = value.trim();

			if (field.startsWith("-"))
			{
				field = value.substring(1);
				isAscending = false;
			}
		}

		public String field()
		{
			return field;
		}

		public boolean isAscending()
		{
			return isAscending;
		}
	}
}

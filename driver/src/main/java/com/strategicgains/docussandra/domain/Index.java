package com.strategicgains.docussandra.domain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.strategicgains.docussandra.Constants;
import com.strategicgains.repoexpress.domain.AbstractTimestampedIdentifiable;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.syntaxe.annotation.ChildValidation;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import com.strategicgains.syntaxe.annotation.Required;

public class Index
extends AbstractTimestampedIdentifiable
{

	@Required("Table")
	@ChildValidation
	private TableReference table;

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

//	@Required("Index Type")
//	private IndexType type;

	/**
	 * Consider the index is only concerned with only a partial dataset. In this case, instead of storing the entire BSON
	 * payload, we store only a subset--those listed in includeOnly.
	 */
	private List<String> includeOnly;

	public Index()
	{
	}

	public Index(String name)
	{
		this();
		name(name);
	}

	public boolean hasTable()
	{
		return (table != null);
	}

	public Table table()
	{
		return (hasTable() ? table.asObject() : null);
	}

	public void table(String database, String table)
	{
		this.table = new TableReference(database, table);
	}

	public void table(Table table)
	{
		this.table = (table != null ? new TableReference(table) : null);
	}

	public String tableName()
	{
		return (hasTable() ? table.name() : null);
	}

	public String databaseName()
	{
		return (hasTable() ? table.database() : null);
	}

	public String name()
	{
		return name;
	}

	public Index name(String name)
	{
		this.name = name;
		return this;
	}

	public boolean isUnique()
	{
		return isUnique;
	}

	public Index isUnique(boolean value)
	{
		this.isUnique = value;
		return this;
	}

	public void fields(List<String> props)
	{
		this.fields = new ArrayList<String>(props);
	}

	public List<String> fields()
	{
		return (fields == null ? Collections.<String>emptyList() : Collections.unmodifiableList(fields));
	}

	public void includeOnly(List<String> props)
	{
		this.includeOnly = new ArrayList<String>(props);
	}

	public List<String> includeOnly()
	{
		return (includeOnly == null ? Collections.<String>emptyList() : Collections.unmodifiableList(includeOnly));
	}

	@Override
    public Identifier getId()
    {
		return new Identifier(databaseName(), tableName(), name);
    }

	@Override
    public void setId(Identifier id)
    {
		// intentionally left blank.
    }

	public long bucketSize()
	{
		return bucketSize;
	}

	public void bucketSize(long bucketSize)
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

			if (field.trim().startsWith("-"))
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

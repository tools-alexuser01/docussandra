package com.strategicgains.docussandra.domain;

import java.nio.ByteBuffer;

import com.strategicgains.repoexpress.domain.AbstractTimestampedIdentifiable;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.syntaxe.annotation.ChildValidation;
import com.strategicgains.syntaxe.annotation.Required;

public class Document
extends AbstractTimestampedIdentifiable
{
	//TODO: add any necessary metadata regarding a document.
	//TODO: documents are versioned per transaction via updateAt timestamp.
	private Key id;

	// need a separate version (as opposed to updatedAt)?
//	private long version;

	@Required("Table")
	@ChildValidation
	private TableReference table;

	// The JSON document.
	private String object;

	public Document()
	{
	}

	public Identifier getId()
	{
		return new Identifier(databaseName(), tableName(), id, getUpdatedAt());
	}

	public void setId(Identifier id)
	{
		// Do nothing. Throw?
	}

	public Key key()
	{
		return id;
	}

	public void key(ByteBuffer bytes)
	{
		this.id = new Key(bytes);
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

	public String object()
	{
		return object;
	}

	public void object(String json)
	{
		this.object = json;
	}
}

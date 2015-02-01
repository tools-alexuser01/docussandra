package com.strategicgains.docussandra.domain;

import java.util.List;

import com.strategicgains.repoexpress.domain.AbstractUuidEntity;
import com.strategicgains.syntaxe.annotation.Required;

public class Query
extends AbstractUuidEntity
{
	private static final int DEFAULT_LIMIT = 100;

//	public Map<String, String> variables;

	@Required("Table name")
	public String table;

	@Required("Columns")
	public List<String> columns;

	public String where;
	public int limit = DEFAULT_LIMIT;
	public int offset;

	public Query()
	{
	}
}

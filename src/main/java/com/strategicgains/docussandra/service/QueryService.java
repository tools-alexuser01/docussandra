package com.strategicgains.docussandra.service;

import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.persistence.QueryRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.syntaxe.ValidationEngine;

public class QueryService
{
	private QueryRepository queries;
	
	public QueryService(QueryRepository queryRepository)
	{
		super();
		this.queries = queryRepository;
	}

	public Query create(Query entity)
	{
		ValidationEngine.validateAndThrow(entity);
		return queries.create(entity);
	}

	public Query read(Identifier id)
    {
		return queries.read(id);
    }

	public void update(Query entity)
    {
		ValidationEngine.validateAndThrow(entity);
		queries.update(entity);
    }

	public void delete(Identifier id)
    {
		queries.delete(id);
    }
}

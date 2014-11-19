package com.strategicgains.docussandra.service;

import java.util.List;

import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.syntaxe.ValidationEngine;

public class IndexService
{
	private IndexRepository indexes;
	
	public IndexService(IndexRepository indexRepository)
	{
		super();
		this.indexes = indexRepository;
	}

	public Index create(Index index)
	{
		ValidationEngine.validateAndThrow(index);
		return indexes.create(index);
	}

	public Index read(Identifier identifier)
    {
		return indexes.read(identifier);
    }

	public void update(Index index)
    {
		ValidationEngine.validateAndThrow(index);
		indexes.update(index);
    }

	public void delete(Identifier identifier)
    {
		indexes.delete(identifier);
    }

	public List<Index> readAll(String context, String nodeType)
    {
	    return indexes.readAll(context, nodeType);
    }

	public long count(String context, String nodeType)
    {
	    return indexes.countAll(context, nodeType);
    }
}

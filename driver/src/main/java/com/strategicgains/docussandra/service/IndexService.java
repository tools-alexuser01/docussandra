package com.strategicgains.docussandra.service;

import java.util.List;

import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.TableRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.exception.ItemNotFoundException;
import com.strategicgains.syntaxe.ValidationEngine;

public class IndexService
{
	private TableRepository tables;
	private IndexRepository indexes;
	
	public IndexService(TableRepository tableRepository, IndexRepository indexRepository)
	{
		super();
		this.indexes = indexRepository;
		this.tables = tableRepository;
	}

	public Index create(Index index)
	{
		verifyTable(index.databaseName(), index.tableName());
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

	public List<Index> readAll(String namespace, String collection)
    {
	    return indexes.readAllCached(namespace, collection);
    }

	public long count(String namespace, String collection)
    {
	    return indexes.countAll(namespace, collection);
    }

	private void verifyTable(String database, String table)
    {
	    Identifier tableId = new Identifier(database, table);

		if (!tables.exists(tableId))
		{
			throw new ItemNotFoundException("Table not found: " + tableId.toString());
		}
    }
}

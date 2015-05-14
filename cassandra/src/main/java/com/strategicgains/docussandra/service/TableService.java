package com.strategicgains.docussandra.service;

import com.strategicgains.docussandra.domain.Identifier;
import java.util.List;

import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.exception.ItemNotFoundException;
import com.strategicgains.docussandra.persistence.DatabaseRepository;
import com.strategicgains.docussandra.persistence.TableRepository;
import com.strategicgains.syntaxe.ValidationEngine;

public class TableService
{
	private TableRepository tables;
	private DatabaseRepository databases;
	
	public TableService(DatabaseRepository databaseRepository, TableRepository tableRepository)
	{
		super();
		this.databases = databaseRepository;
		this.tables = tableRepository;
	}

	public Table create(Table entity)
	{
		if (!databases.exists(entity.database().getId()))
		{
			throw new ItemNotFoundException("Database not found: " + entity.database());
		}

		ValidationEngine.validateAndThrow(entity);
		return tables.createEntity(entity);
	}

	public Table read(String database, String table)
	{
		Identifier id = new Identifier(database, table);
		Table t = tables.readEntityById(id);

		if (t == null) throw new ItemNotFoundException("Table not found: " + id.toString());

		return t;
	}

	public List<Table> readAll(String database)
	{
		if (!databases.exists(new Identifier(database))) throw new ItemNotFoundException("Database not found: " + database);

		return tables.readAll(database);
	}

	public void update(Table entity)
    {
		ValidationEngine.validateAndThrow(entity);
		tables.updateEntity(entity);
    }

	public void delete(Identifier id)
    {
		tables.deleteEntity(id);
    }
}

package com.strategicgains.docussandra.service;

import java.util.List;

import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.IndexCreationStatus;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.TableRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.exception.ItemNotFoundException;
import com.strategicgains.syntaxe.ValidationEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexService
{

    private TableRepository tables;
    private IndexRepository indexes;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

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
        logger.debug("Creating index: " + index.toString());
        return indexes.create(index);
    }

    public Index read(Identifier identifier)
    {
        return indexes.read(identifier);
    }

    public void update(Index index)
    {
        ValidationEngine.validateAndThrow(index);
        logger.debug("Updating index: " + index.toString());
        indexes.update(index);
    }

    public void delete(Identifier identifier)
    {
        logger.debug("Deleting index: " + identifier.toString());
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

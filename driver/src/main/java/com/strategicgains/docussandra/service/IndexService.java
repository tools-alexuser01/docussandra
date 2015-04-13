package com.strategicgains.docussandra.service;

import java.util.List;

import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.IndexCreationStatus;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.IndexStatusRepository;
import com.strategicgains.docussandra.persistence.TableRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.exception.ItemNotFoundException;
import com.strategicgains.syntaxe.ValidationEngine;
import java.util.Date;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexService
{

    private TableRepository tables;
    private IndexRepository indexes;
    private IndexStatusRepository status;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public IndexService(TableRepository tableRepository, IndexRepository indexRepository, IndexStatusRepository status)
    {
        super();
        this.indexes = indexRepository;
        this.tables = tableRepository;
        this.status = status;
    }

    public IndexCreationStatus create(Index index)
    {
        verifyTable(index.databaseName(), index.tableName());
        ValidationEngine.validateAndThrow(index);
        index.setActive(false);//we default to not active when being created; we don't allow the user to change this; only the app can change this
        logger.debug("Creating index: " + index.toString());
        Index created = indexes.create(index);
        long dataSize = tables.countTableSize(index.databaseName(), index.tableName());
        Date now = new Date();
        UUID uuid = UUID.randomUUID();
        return new IndexCreationStatus(uuid, now, now, created, dataSize, 0l);
    }

    /**
     * Gets the status of an index creation event.
     * @param id Id to get the status for.
     * @return an IndexCreationStatus for this id.
     */
    public IndexCreationStatus status(UUID id)
    {
        logger.debug("Checking index creation status: " + id.toString());
        return status(id);
    }
    
    /**
     * Gets all statuses for pending index creations.
     * @return a list of IndexCreationStatus.
     */
    public List<IndexCreationStatus> getAllActiveStatus()
    {
        logger.debug("Checking index creation status.");
        return status.readAllActive();
    }

    public Index read(Identifier identifier)
    {
        return indexes.read(identifier);
    }

    public void delete(Identifier identifier)
    {
        logger.debug("Deleting index: " + identifier.toString());
        indexes.delete(identifier);
    }

    public void delete(Index index)
    {
        Identifier identifier = index.getId();
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

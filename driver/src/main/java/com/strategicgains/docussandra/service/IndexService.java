package com.strategicgains.docussandra.service;

import java.util.List;

import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.IndexCreationStatus;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.IndexStatusRepository;
import com.strategicgains.docussandra.persistence.TableRepository;
import com.strategicgains.eventing.DomainEvents;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.exception.ItemNotFoundException;
import com.strategicgains.syntaxe.ValidationEngine;
import java.util.Date;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexService
{

    private TableRepository tablesRepo;
    private IndexRepository indexesRepo;
    private IndexStatusRepository statusRepo;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public IndexService(TableRepository tableRepository, IndexRepository indexRepository, IndexStatusRepository status)
    {
        super();
        this.indexesRepo = indexRepository;
        this.tablesRepo = tableRepository;
        this.statusRepo = status;
    }

    public IndexCreationStatus create(Index index)
    {
        verifyTable(index.databaseName(), index.tableName());
        ValidationEngine.validateAndThrow(index);
        index.setActive(false);//we default to not active when being created; we don't allow the user to change this; only the app can change this
        logger.debug("Creating index: " + index.toString());
        Index created = indexesRepo.create(index);
        long dataSize = tablesRepo.countTableSize(index.databaseName(), index.tableName());
        Date now = new Date();
        UUID uuid = UUID.randomUUID();//TODO: is this right?
        IndexCreationStatus toReturn = new IndexCreationStatus(uuid, now, now, created, dataSize, 0l);
        if(!statusRepo.exists(uuid)){
            statusRepo.createEntity(toReturn);
        }
        DomainEvents.publish(uuid);
        return toReturn;
    }

    /**
     * Gets the statusRepo of an index creation event.
     * @param id Id to get the statusRepo for.
     * @return an IndexCreationStatus for this id.
     */
    public IndexCreationStatus status(UUID id)
    {
        logger.debug("Checking index creation status: " + id.toString());
        return statusRepo.readEntityByUUID(id);
    }
    
    /**
     * Gets all statuses for pending index creations.
     * @return a list of IndexCreationStatus.
     */
    public List<IndexCreationStatus> getAllActiveStatus()
    {
        logger.debug("Checking index creation status.");
        return statusRepo.readAllActive();
    }

    public Index read(Identifier identifier)
    {
        return indexesRepo.read(identifier);
    }

    public void delete(Identifier identifier)
    {
        logger.debug("Deleting index: " + identifier.toString());
        indexesRepo.delete(identifier);
    }

    public void delete(Index index)
    {
        Identifier identifier = index.getId();
        logger.debug("Deleting index: " + identifier.toString());
        indexesRepo.delete(identifier);
    }

    public List<Index> readAll(String namespace, String collection)
    {
        return indexesRepo.readAllCached(namespace, collection);
    }

    public long count(String namespace, String collection)
    {
        return indexesRepo.countAll(namespace, collection);
    }

    private void verifyTable(String database, String table)
    {
        Identifier tableId = new Identifier(database, table);

        if (!tablesRepo.exists(tableId))
        {
            throw new ItemNotFoundException("Table not found: " + tableId.toString());
        }
    }
}

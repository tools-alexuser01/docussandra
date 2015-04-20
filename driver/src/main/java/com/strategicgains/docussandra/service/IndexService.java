package com.strategicgains.docussandra.service;

import java.util.List;

import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.event.IndexCreatedEvent;
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

/**
 * Service for interacting with indexes.
 *
 * @author udeyoje
 */
public class IndexService
{

    /**
     * Table repository to use for interacting with tables.
     */
    private TableRepository tablesRepo;

    /**
     * Index repository to use for interacting with indexes.
     */
    private IndexRepository indexesRepo;

    /**
     * IndexStatus repository to use for interacting with index statuses.
     */
    private IndexStatusRepository statusRepo;

    /**
     * Logger for this class.
     */
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Constructor.
     *
     * @param tableRepository Table repository to use for interacting with
     * tables.
     * @param indexRepository Index repository to use for interacting with
     * indexes.
     * @param status IndexStatus repository to use for interacting with index
     * statuses.
     */
    public IndexService(TableRepository tableRepository, IndexRepository indexRepository, IndexStatusRepository status)
    {
        super();
        this.indexesRepo = indexRepository;
        this.tablesRepo = tableRepository;
        this.statusRepo = status;
    }

    /**
     * Creates an index. Index will be created synchronously, but it will be
     * populated asynchronously, and it will not be "active" (meaning it can't
     * be used for querying) until indexing is complete.
     *
     * @param index Index to create.
     * @return An IndexCreatedEvent that contains the index and some metadata
     * about it's creation status.
     */
    public IndexCreatedEvent create(Index index)
    {
        verifyTable(index.databaseName(), index.tableName());
        ValidationEngine.validateAndThrow(index);
        index.setActive(false);//we default to not active when being created; we don't allow the user to change this; only the app can change this
        logger.debug("Creating index: " + index.toString());
        Index created = indexesRepo.create(index);
        long dataSize = tablesRepo.countTableSize(index.databaseName(), index.tableName());
        Date now = new Date();
        UUID uuid = UUID.randomUUID();//TODO: is this right?
        IndexCreatedEvent toReturn = new IndexCreatedEvent(uuid, now, now, created, dataSize, 0l);
        if (!statusRepo.exists(uuid))
        {
            statusRepo.createEntity(toReturn);
        }
        toReturn.calculateValues();
        DomainEvents.publish(toReturn);
        return toReturn;
    }

    /**
     * Gets the status of an index creation event. Allows a user to check on the
     * status of the indexing for an index.
     *
     * @param id Id to get the status for.
     * @return an IndexCreatedEvent for this id.
     */
    public IndexCreatedEvent status(UUID id)
    {
        logger.debug("Checking index creation status: " + id.toString());
        IndexCreatedEvent toReturn = statusRepo.readEntityByUUID(id);
        return toReturn;
    }

    /**
     * Gets statuses for ALL pending index creations. Allows users and admins to
     * check the indexing load on the system.
     *
     * @return a list of IndexCreatedEvent.
     */
    public List<IndexCreatedEvent> getAllCurrentlyIndexing()
    {
        logger.debug("Checking index creation status.");
        return statusRepo.readAllCurrentlyIndexing();
    }

    /**
     * Reads an index.
     * @param identifier
     * @return 
     */
    public Index read(Identifier identifier)
    {
        return indexesRepo.read(identifier);
    }

    /**
     * Deletes an index. Will also remove the associated iTables.
     * @param identifier 
     */
    public void delete(Identifier identifier)
    {
        logger.debug("Deleting index: " + identifier.toString());
        indexesRepo.delete(identifier);
    }

    /**
     * Deletes an index. Will also remove the associated iTables.
     * @param index 
     */
    public void delete(Index index)
    {
        Identifier identifier = index.getId();
        logger.debug("Deleting index: " + identifier.toString());
        indexesRepo.delete(identifier);
    }

    /**
     * Reads all indexes for the given namespace and collection.
     * @param namespace
     * @param collection
     * @return 
     */
    public List<Index> readAll(String namespace, String collection)
    {
        return indexesRepo.readAllCached(namespace, collection);
    }

    /**
     * Counts the number of indexes for this namespace and collection.
     * @param namespace
     * @param collection
     * @return 
     */
    public long count(String namespace, String collection)
    {
        return indexesRepo.countAll(namespace, collection);
    }

    /**
     * Verifies if a table exists or not.
     * @param database
     * @param table 
     */
    private void verifyTable(String database, String table)
    {
        Identifier tableId = new Identifier(database, table);

        if (!tablesRepo.exists(tableId))
        {
            throw new ItemNotFoundException("Table not found: " + tableId.toString());
        }
    }
}

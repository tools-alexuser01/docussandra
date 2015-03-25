package com.strategicgains.docussandra.handler;

import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.event.IndexDeletedEvent;
import com.strategicgains.docussandra.persistence.ITableRepository;
import com.strategicgains.eventing.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author toddf
 * @since Nov 19, 2014
 */
public class IndexDeletedHandler
        implements EventHandler
{

    private Session dbSession;
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexDeletedHandler.class);

    public IndexDeletedHandler(Session dbSession)
    {
        this.dbSession = dbSession;
    }

    @Override
    public void handle(Object event) throws Exception
    {
        handle((IndexDeletedEvent) event);
    }

    public void handle(IndexDeletedEvent event)
    {
        LOGGER.info("Cleaning up ITables for index: " + event.data.databaseName() + "/" + event.data.tableName() + "/" + event.data.name());
        ITableRepository itr = new ITableRepository(dbSession);
        Index toDelete = event.data;
        if(itr.iTableExists(toDelete)){
            itr.deleteITable(toDelete);
        }
        
    }

    @Override
    public boolean handles(Class<?> eventClass)
    {
        return IndexDeletedEvent.class.isAssignableFrom(eventClass);
    }
}

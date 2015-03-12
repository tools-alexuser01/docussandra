package com.strategicgains.docussandra.handler;

import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.event.IndexDeletedEvent;
import com.strategicgains.docussandra.persistence.ITableRepository;
import com.strategicgains.eventing.EventHandler;

/**
 *
 * @author toddf
 * @since Nov 19, 2014
 */
public class IndexDeletedHandler
        implements EventHandler
{

    private Session dbSession;

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

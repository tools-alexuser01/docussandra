package com.strategicgains.docussandra.handler;

import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.event.TableDeletedEvent;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.eventing.EventHandler;
import java.util.List;

/**
 *
 * @author udeyoje
 * @since Nov 19, 2014
 */
public class TableDeleteHandler
        implements EventHandler
{

    private Session dbSession;

    public TableDeleteHandler(Session dbSession)
    {
        this.dbSession = dbSession;
    }   
    

    @Override
    public void handle(Object event) throws Exception
    {
        handle((TableDeletedEvent) event);
    }

    public void handle(TableDeletedEvent event)
    {
        //remove all the collections and all the documents in that table.
        //TODO: version instead of delete
        //Delete all indexes
        IndexRepository ir = new IndexRepository(dbSession);
        List<Index> indexes = ir.readAll(event.data.databaseName(), event.data.name());//get all indexes
        for(Index i : indexes){
            ir.doDelete(i);// then delete them
        }
    }

    @Override
    public boolean handles(Class<?> eventClass)
    {
        return TableDeletedEvent.class.isAssignableFrom(eventClass);
    }
}

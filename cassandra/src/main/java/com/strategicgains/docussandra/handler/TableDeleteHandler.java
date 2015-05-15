package com.strategicgains.docussandra.handler;

import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.event.TableDeletedEvent;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.eventing.EventHandler;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author udeyoje
 * @since Nov 19, 2014
 */
@Deprecated
public class TableDeleteHandler
        implements EventHandler
{

    private Session dbSession;
    private static final Logger LOGGER = LoggerFactory.getLogger(TableDeleteHandler.class);

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
        LOGGER.info("Cleaning up Indexes for table: " + event.data.databaseName() + "/" + event.data.name());
        //remove all the tables and all the documents in that table.
        //TODO: version instead of delete
        //Delete all indexes
        IndexRepository ir = new IndexRepository(dbSession);
        List<Index> indexes = ir.readAll(event.data.getId());//get all indexes
        for(Index i : indexes){
            ir.delete(i);// then delete them
        }
    }

    @Override
    public boolean handles(Class<?> eventClass)
    {
        return TableDeletedEvent.class.isAssignableFrom(eventClass);
    }
}

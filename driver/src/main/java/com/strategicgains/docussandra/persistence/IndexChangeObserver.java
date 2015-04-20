package com.strategicgains.docussandra.persistence;

import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.repoexpress.domain.Identifiable;
import com.strategicgains.repoexpress.event.AbstractRepositoryObserver;

/**
 * Observer of any changes to the index table (sys_idx). Propagates any needed
 * index changes to the iTables.
 *
 * @author udeyoje
 * @since Feb 12, 2015
 */
public class IndexChangeObserver<T extends Identifiable>
        extends AbstractRepositoryObserver<T>
{

    /**
     * Session for interacting with the Cassandra database.
     */
    private Session session;

    /**
     * Dao for interacting with the iTables.
     */
    private ITableRepository dao;

    /**
     * Constructor. Creates a new IndexChangeObserver.
     *
     * @param session Session for interacting with the Cassandra database.
     */
    public IndexChangeObserver(Session session)
    {
        super();
        this.session = session;
        //TODO: check thread safety here
        dao = new ITableRepository(session);

    }

    @Override
    public void afterCreate(T object)
    {
        //create the iTable
        Index index = (Index) object;
        if (!dao.iTableExists(index))
        {
            dao.createITable(index);
        }
        //TODO: what if it already exists?
        //options:
        //-----check to see if it is correct, suggest the user delete and try again if it's not -- probably
        //-----automatically re-index; hard to actually do, it would need a different name if the index was in use -- probably not
        //-----do nothing -- maybe?
        
        //note: index population will be called from the service layer
    }

    @Override
    public void afterDelete(T object)
    {
        //drop the iTable
        Index index = (Index) object;
        if (dao.iTableExists(index))
        {
            dao.deleteITable(index);
        }
        //TODO: what if it doesn't exist?
        //options:
        //-----warn the user -- probably not
        //-----throw an exception -- probably not even more
        //-----do nothing -- probably
    }

}

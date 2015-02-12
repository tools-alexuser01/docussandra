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
        extends AbstractRepositoryObserver<T> {

    /**
     * Session for interacting with the Cassandra database.
     */
    private Session session;

    /**
     * Constructor. Creates a new IndexChangeObserver.
     *
     * @param session Session for interacting with the Cassandra database.
     */
    public IndexChangeObserver(Session session) {
        super();
        this.session = session;
    }

    @Override
    public void afterCreate(T object) {
        //create the iTable
        Index index = (Index) object;
    }

    @Override
    public void afterDelete(T object) {
        //drop the iTable
        Index index = (Index) object;
    }

    @Override
    public void afterUpdate(T object) {
        Index index = (Index) object;
        //TODO: (maybe) schedule job to handle this at a later time (off hours); very expensive operation
        //drop the iTable
        //recreate the iTable
        //repopulate the iTable with existing data
    }
}

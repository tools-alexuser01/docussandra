package com.strategicgains.docussandra.persistence;

import com.strategicgains.repoexpress.domain.Identifiable;
import com.strategicgains.repoexpress.event.AbstractRepositoryObserver;

/**
 * @author udeyoje
 * @since Feb 12, 2015
 */
public class IndexChangeObserver<T extends Identifiable>
        extends AbstractRepositoryObserver<T> {

    public IndexChangeObserver() {
        super();
    }

    @Override
    public void afterCreate(T object) {
        //create the iTable
    }

    @Override
    public void afterDelete(T object) {
        //drop the iTable
    }

    @Override
    public void afterUpdate(T object) {
        //TODO: schedule job to handle this at a later time (off hours); very expensive operation
        //drop the iTable
        //recreate the iTable
        //repopulate the iTable with existing data
    }
}

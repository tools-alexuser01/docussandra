package com.strategicgains.docussandra.persistence;

import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.Index;

/**
 * Data Access Object for manipulating iTables (tables that contain the actual
 * index data). This is a normal DAO instead of part of the persistence
 * framework due to the dynamic nature of creating these tables.
 *
 * @author udeyoje
 * @since Feb 12, 2015
 */
public class ITableDao {

    /**
     * Session for interacting with the Cassandra database.
     */
    private Session session;

    /**
     * Constructor. Creates a new ITableDao.
     * @param session Session for interacting with the Cassandra database.
     */
    public ITableDao(Session session) {
        this.session = session;
    }
    
    public boolean iTableExists(Index index){
        throw new UnsupportedOperationException("Not done yet");
    }
    
    public void createITable(Index index){
        throw new UnsupportedOperationException("Not done yet");
    }
    
    public void deleteITable(Index index){
        throw new UnsupportedOperationException("Not done yet");
    }
    
    
    
    
    

    
}

package com.strategicgains.docussandra.persistence;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.Utils;
import com.strategicgains.docussandra.domain.Index;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * Logger for this class.
     */
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String TABLE_EXISTENCE_CQL = "select columnfamily_name from system.schema_columnfamilies where columnfamily_name = ? ALLOW FILTERING;";

    /**
     * Constructor. Creates a new ITableDao.
     *
     * @param session Session for interacting with the Cassandra database.
     */
    public ITableDao(Session session) {
        this.session = session;
    }

    public boolean iTableExists(Index index) {
        PreparedStatement createStmt = session.prepare(TABLE_EXISTENCE_CQL);

        BoundStatement bs = new BoundStatement(createStmt);
        bs.bind(Utils.calculateITableName(index));
        ResultSet rs = session.execute(bs);
        Iterator ite = rs.iterator();
        
        while (ite.hasNext()) {
            logger.debug(ite.next().toString());
            return true;
        }
        return false;
        
    }

    public void createITable(Index index) {
        throw new UnsupportedOperationException("Not done yet");
    }

    public void deleteITable(Index index) {
        throw new UnsupportedOperationException("Not done yet");
    }

}

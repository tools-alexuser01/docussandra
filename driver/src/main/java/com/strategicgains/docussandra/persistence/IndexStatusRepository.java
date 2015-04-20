package com.strategicgains.docussandra.persistence;

import com.datastax.driver.core.BatchStatement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.IndexCreationStatus;
import com.strategicgains.docussandra.persistence.helper.PreparedStatementFactory;
import com.strategicgains.repoexpress.exception.ItemNotFoundException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repository for interacting with the sys_idx_status and sys_idx_not_done
 * tables.
 * TODO: Javadoc
 * @author udeyoje
 */
public class IndexStatusRepository
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private IndexRepository indexRepo;

    public class Tables
    {

        public static final String BY_ID = "sys_idx_status";
        public static final String BY_NOT_DONE = "sys_idx_not_done";
    }

    private class Columns
    {

        static final String ID = "id";
        static final String DATABASE = "db_name";
        static final String TABLE = "tbl_name";
        static final String INDEX_NAME = "index_name";
        static final String TOTAL_RECORDS = "total_records";
        static final String RECORDS_COMPLETED = "records_completed";
        static final String STARTED_AT = "started_at";
        static final String UPDATED_AT = "updated_at";
        static final String ERROR = "error";
    }

    private static final String IDENTITY_CQL = " where id = ?";
    private static final String EXISTENCE_CQL = "select count(*) from " + Tables.BY_ID + IDENTITY_CQL;
    private static final String DELETE_FROM_NOT_DONE = "delete from " + Tables.BY_NOT_DONE + IDENTITY_CQL;
    private static final String CREATE_CQL = "insert into " + Tables.BY_ID + " (" + Columns.ID + ", " + Columns.DATABASE + ", " + Columns.TABLE + ", " + Columns.INDEX_NAME + ", " + Columns.RECORDS_COMPLETED + ", " + Columns.TOTAL_RECORDS + ", " + Columns.STARTED_AT + ", " + Columns.UPDATED_AT + ", " + Columns.ERROR + ") values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String READ_CQL = "select * from " + Tables.BY_ID + IDENTITY_CQL;
    private static final String UPDATE_CQL = "update " + Tables.BY_ID + " set " + Columns.RECORDS_COMPLETED + " = ?, " + Columns.UPDATED_AT + " = ?, " + Columns.ERROR + " = ?" + IDENTITY_CQL;
    private static final String MARK_INDEXING_CQL = "insert into " + Tables.BY_NOT_DONE + "(" + Columns.ID + ") values (?)";//TODO: if not exists?

    private static final String READ_ALL_CQL = "select * from " + Tables.BY_ID;
    private static final String READ_ALL_CURRENTLY_INDEXING_CQL = "select * from " + Tables.BY_NOT_DONE;
    private static final String IS_CURRENTLY_INDEXING_CQL = "select count(*) from " + Tables.BY_NOT_DONE + IDENTITY_CQL;//records that are currently indexing (not yet done)

    private PreparedStatement existStmt;
    private PreparedStatement readStmt;
    private PreparedStatement createStmt;
    private PreparedStatement updateStmt;
    private PreparedStatement readAllStmt;
    private PreparedStatement markIndexingStmt;
    private PreparedStatement readAllCurrentlyIndexingStmt;
    private PreparedStatement deleteFromNotDoneStmt;
    private PreparedStatement isCurrentlyIndexingStmt;
    
    private Session session;

    public IndexStatusRepository(Session session)
    {
        this.session = session;
        initialize();
        indexRepo = new IndexRepository(session);
    }

    protected void initialize()
    {
        existStmt = PreparedStatementFactory.getPreparedStatement(EXISTENCE_CQL, getSession());
        readStmt = PreparedStatementFactory.getPreparedStatement(READ_CQL, getSession());
        createStmt = PreparedStatementFactory.getPreparedStatement(CREATE_CQL, getSession());
        updateStmt = PreparedStatementFactory.getPreparedStatement(UPDATE_CQL, getSession());
        readAllStmt = PreparedStatementFactory.getPreparedStatement(READ_ALL_CQL, getSession());
        readAllCurrentlyIndexingStmt = PreparedStatementFactory.getPreparedStatement(READ_ALL_CURRENTLY_INDEXING_CQL, getSession());
        deleteFromNotDoneStmt = PreparedStatementFactory.getPreparedStatement(DELETE_FROM_NOT_DONE, getSession());
        markIndexingStmt = PreparedStatementFactory.getPreparedStatement(MARK_INDEXING_CQL, getSession());
        isCurrentlyIndexingStmt = PreparedStatementFactory.getPreparedStatement(IS_CURRENTLY_INDEXING_CQL, getSession());
    }


    public boolean exists(UUID uuid)
    {
        if (uuid == null)
        {
            return false;
        }
        BoundStatement bs = new BoundStatement(existStmt);
        bindUUIDWhere(bs, uuid);
        return (getSession().execute(bs).one().getLong(0) > 0);
    }


    public IndexCreationStatus readEntityByUUID(UUID uuid)
    {
        if (uuid == null)
        {
            return null;
        }
        BoundStatement bs = new BoundStatement(readStmt);
        bindUUIDWhere(bs, uuid);
        return marshalRow(getSession().execute(bs).one());
    }

    public IndexCreationStatus createEntity(IndexCreationStatus entity)
    {
        BoundStatement create = new BoundStatement(createStmt);
        bindCreate(create, entity);
        BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
        if (!entity.isDoneIndexing())
        {
            markIndexing(entity.getUuid());
        }
        batch.add(create);
        getSession().execute(batch);
        return entity;
    }

    public IndexCreationStatus updateEntity(IndexCreationStatus entity)
    {
        BoundStatement bs = new BoundStatement(updateStmt);
        bindUpdate(bs, entity);
        if (entity.isDoneIndexing())
        {
            markDone(entity.getUuid());
        } else
        {
            markIndexing(entity.getUuid());
        }
        getSession().execute(bs);
        return entity;
    }

    private void markIndexing(UUID id)
    {
        BoundStatement activeStatement = new BoundStatement(markIndexingStmt);
        activeStatement.bind(id);
        getSession().execute(activeStatement);
    }

    private void markDone(UUID id)
    {
        BoundStatement delete = new BoundStatement(deleteFromNotDoneStmt);
        bindUUIDWhere(delete, id);
        getSession().execute(delete);
    }

    public List<IndexCreationStatus> readAll()
    {
        BoundStatement bs = new BoundStatement(readAllStmt);
        return (marshalAll(getSession().execute(bs)));
    }

    public List<IndexCreationStatus> readAllCurrentlyIndexing()
    {
        BoundStatement bs = new BoundStatement(readAllCurrentlyIndexingStmt);
        List<UUID> ids = marshalActiveUUIDs(getSession().execute(bs));
        List<IndexCreationStatus> toReturn = new ArrayList<>(ids.size());
        for (UUID id : ids)
        {
            toReturn.add(readEntityByUUID(id));
        }
        return toReturn;
    }

    private boolean isCurrentlyIndexing(UUID id)
    {
        BoundStatement bs = new BoundStatement(isCurrentlyIndexingStmt);
        bindUUIDWhere(bs, id);
        return (getSession().execute(bs).one().getLong(0) > 0);
    }

//    public long countAll(String namespace, String collection)
//    {
//        BoundStatement bs = new BoundStatement(readAllCountStmt);
//        bs.bind(namespace, collection);
//        return (getSession().execute(bs).one().getLong(0));
//    }
    private void bindCreate(BoundStatement bs, IndexCreationStatus entity)
    {
        bs.bind(entity.getUuid(),
                entity.getIndex().databaseName(),
                entity.getIndex().tableName(),
                entity.getIndex().name(),
                entity.getRecordsCompleted(),
                entity.getTotalRecords(),
                entity.getDateStarted(),
                entity.getStatusLastUpdatedAt(),
                entity.getError());
    }

    private void bindUpdate(BoundStatement bs, IndexCreationStatus entity)
    {
        bs.bind(entity.getRecordsCompleted(),
                entity.getStatusLastUpdatedAt(),
                entity.getError(),
                entity.getUuid());
    }

    protected void bindUUIDWhere(BoundStatement bs, UUID uuid)
    {
        bs.bind(uuid);
    }

    private List<IndexCreationStatus> marshalAll(ResultSet rs)
    {
        List<IndexCreationStatus> indexes = new ArrayList<>();
        Iterator<Row> i = rs.iterator();
        while (i.hasNext())
        {
            IndexCreationStatus status = marshalRow(i.next());
            indexes.add(status);
        }
        return indexes;
    }

    private List<UUID> marshalActiveUUIDs(ResultSet rs)
    {
        List<UUID> activeIds = new ArrayList<>();
        Iterator<Row> i = rs.iterator();
        while (i.hasNext())
        {
            activeIds.add(i.next().getUUID(Columns.ID));
        }
        return activeIds;
    }

    protected IndexCreationStatus marshalRow(Row row)
    {
        if (row == null)
        {
            return null;
        }        
        //look up index here
        Index index = new Index();
        index.name(row.getString(Columns.INDEX_NAME));
        index.table(row.getString(Columns.DATABASE), row.getString(Columns.TABLE));
        Index toUse;
        try
        {
            toUse = indexRepo.doRead(index.getId());
        } catch (ItemNotFoundException e)//this should only happen in tests that do not have full test data established; errors will be evident if this happens in the actual app
        {
             toUse = index;
        }
        IndexCreationStatus i = new IndexCreationStatus(row.getUUID(Columns.ID), row.getDate(Columns.STARTED_AT), row.getDate(Columns.UPDATED_AT), toUse, row.getLong(Columns.TOTAL_RECORDS), row.getLong(Columns.RECORDS_COMPLETED));
        i.setError(row.getString(Columns.ERROR));
        i.calculateValues();
        return i;
    }

    public Session getSession()
    {
        return session;
    }

}

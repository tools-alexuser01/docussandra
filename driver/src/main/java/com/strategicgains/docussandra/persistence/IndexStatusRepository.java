package com.strategicgains.docussandra.persistence;

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
import com.strategicgains.repoexpress.cassandra.AbstractCassandraRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexStatusRepository
        extends AbstractCassandraRepository<IndexCreationStatus>
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private class Tables
    {

        static final String BY_ID = "sys_idx_status";
    }

    private class Columns
    {

        static final String ID = "id";
        static final String DATABASE = "db_name";
        static final String TABLE = "tbl_name";
        static final String INDEX_NAME = "index_name";
        static final String RECORDS_COMPLETED = "records_completed";
        static final String IS_DONE = "is_done";
        static final String STARTED_AT = "started_at";
        static final String UPDATED_AT = "updated_at";
    }

    private static final String IDENTITY_CQL = " where id = ?";
    private static final String EXISTENCE_CQL = "select count(*) from " + Tables.BY_ID + IDENTITY_CQL;
    private static final String CREATE_CQL = "insert into " + Tables.BY_ID + " (" + Columns.ID + ", " + Columns.DATABASE + ", " + Columns.TABLE + ", " + Columns.INDEX_NAME + ", " + Columns.RECORDS_COMPLETED + ", " + Columns.IS_DONE + ", " + Columns.STARTED_AT + ", " + Columns.UPDATED_AT + ") values (?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String READ_CQL = "select * from " + Tables.BY_ID + IDENTITY_CQL;
    private static final String UPDATE_CQL = "update " + Tables.BY_ID + " set " + Columns.RECORDS_COMPLETED + " = ?, " + Columns.IS_DONE + " = ?, updated_at = ?" + IDENTITY_CQL;
    private static final String READ_ALL_CQL = "select * from " + Tables.BY_ID;

    private PreparedStatement existStmt;
    private PreparedStatement readStmt;
    private PreparedStatement createStmt;
    private PreparedStatement updateStmt;
    private PreparedStatement readAllStmt;
    private PreparedStatement readAllCountStmt;

    public IndexStatusRepository(Session session)
    {
        super(session, Tables.BY_ID);
        addObserver(new IndexChangeObserver(session));
        initialize();
    }

    protected void initialize()
    {
        existStmt = PreparedStatementFactory.getPreparedStatement(String.format(EXISTENCE_CQL, getTable()), getSession());
        readStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_CQL, getTable()), getSession());
        createStmt = PreparedStatementFactory.getPreparedStatement(String.format(CREATE_CQL, getTable(), Columns.INDEX_NAME), getSession());
        updateStmt = PreparedStatementFactory.getPreparedStatement(String.format(UPDATE_CQL, getTable()), getSession());
        readAllStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_ALL_CQL, getTable()), getSession());
    }

    @Override
    public boolean exists(Identifier identifier)
    {
        if (identifier == null || identifier.isEmpty())
        {
            return false;
        }

        BoundStatement bs = new BoundStatement(existStmt);
        bindIdentifier(bs, identifier);
        return (getSession().execute(bs).one().getLong(0) > 0);
    }

    @Override
    protected IndexCreationStatus readEntityById(Identifier identifier)
    {
        if (identifier == null || identifier.isEmpty())
        {
            return null;
        }

        BoundStatement bs = new BoundStatement(readStmt);
        bindIdentifier(bs, identifier);
        return marshalRow(getSession().execute(bs).one());
    }

    @Override
    protected IndexCreationStatus createEntity(IndexCreationStatus entity)
    {
        BoundStatement bs = new BoundStatement(createStmt);
        bindCreate(bs, entity);
        getSession().execute(bs);
        return entity;
    }

    @Override
    protected IndexCreationStatus updateEntity(IndexCreationStatus entity)
    {
        BoundStatement bs = new BoundStatement(updateStmt);
        bindUpdate(bs, entity);
        getSession().execute(bs);
        return entity;
    }

    @Override
    public void deleteEntity(final IndexCreationStatus id)
    {
        throw new UnsupportedOperationException("This not a valid call; we do not delete past status.");
    }

    public List<IndexCreationStatus> readAll(String namespace, String collection)
    {
        BoundStatement bs = new BoundStatement(readAllStmt);
        bs.bind(namespace, collection);
        return (marshalAll(getSession().execute(bs)));
    }

    public long countAll(String namespace, String collection)
    {
        BoundStatement bs = new BoundStatement(readAllCountStmt);
        bs.bind(namespace, collection);
        return (getSession().execute(bs).one().getLong(0));
    }

    private void bindCreate(BoundStatement bs, IndexCreationStatus entity)
    {
        bs.bind(entity.getUuid(),
                entity.getIndex().databaseName(),
                entity.getIndex().tableName(),
                entity.getIndex().name(),
                entity.getRecordsCompleted(),
                entity.isDone(),
                entity.getDateStarted(),
                entity.getStatusLastUpdatedAt());
    }

    private void bindUpdate(BoundStatement bs, IndexCreationStatus entity)
    {
        bs.bind(entity.getRecordsCompleted(),
                entity.isDone(),
                entity.getUuid());
    }

    private List<IndexCreationStatus> marshalAll(ResultSet rs)
    {
        List<IndexCreationStatus> indexes = new ArrayList<>();
        Iterator<Row> i = rs.iterator();

        while (i.hasNext())
        {
            indexes.add(marshalRow(i.next()));
        }

        return indexes;
    }

    protected IndexCreationStatus marshalRow(Row row)
    {
        if (row == null)
        {
            return null;
        }
        IndexCreationStatus i = new IndexCreationStatus();
        i.setUuid(row.getUUID(Columns.ID));
        Index index = new Index();
        index.name(row.getString(Columns.INDEX_NAME));
        index.table(row.getString(Columns.DATABASE), row.getString(Columns.TABLE));
        i.setIndex(index);
        i.setRecordsCompleted(row.getLong(Columns.RECORDS_COMPLETED));
        i.setDateStarted(row.getDate(Columns.STARTED_AT));
        i.setStatusLastUpdatedAt(row.getDate(Columns.UPDATED_AT));
        return i;
    }
}

package com.strategicgains.docussandra.persistence;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.event.EventFactory;
import com.strategicgains.docussandra.event.TableCreatedEvent;
import com.strategicgains.docussandra.event.TableDeletedEvent;
import com.strategicgains.docussandra.event.TableUpdatedEvent;
import com.strategicgains.docussandra.persistence.helper.PreparedStatementFactory;
import com.strategicgains.repoexpress.cassandra.AbstractCassandraRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.event.DefaultTimestampedIdentifiableRepositoryObserver;

public class TableRepository
        extends AbstractCassandraRepository<Table>
{

    private class Tables
    {

        static final String BY_ID = "sys_tbl";
    }

    private class Columns
    {

        static final String NAME = "tbl_name";
        static final String DATABASE = "db_name";
        static final String DESCRIPTION = "description";
        static final String CREATED_AT = "created_at";
        static final String UPDATED_AT = "updated_at";
    }

    private static final String IDENTITY_CQL = " where " + Columns.DATABASE + " = ? and " + Columns.NAME + " = ?";
    private static final String EXISTENCE_CQL = "select count(*) from %s" + IDENTITY_CQL;
    private static final String CREATE_CQL = "insert into %s (%s, " + Columns.DATABASE + ", " + Columns.DESCRIPTION + ", " + Columns.CREATED_AT + ", " + Columns.UPDATED_AT + ") values (?, ?, ?, ?, ?)";
    private static final String READ_CQL = "select * from %s" + IDENTITY_CQL;
    private static final String DELETE_CQL = "delete from %s" + IDENTITY_CQL;
    private static final String UPDATE_CQL = "update %s set " + Columns.DESCRIPTION + " = ?, " + Columns.UPDATED_AT + " = ?" + IDENTITY_CQL;
    private static final String READ_ALL_CQL = "select * from %s where " + Columns.DATABASE + " = ?";
    private static final String READ_ALL_COUNT_CQL = "select count(*) from %s where " + Columns.DATABASE + " = ?";
    private static final String READ_COUNT_TABLE_SIZE_CQL = "select count(*) from %s where " + Columns.DATABASE + " = ? and " + Columns.NAME + " = ?";

    private static final String CREATE_DOC_TABLE_CQL = "create table %s"
            + " (id uuid, object blob, " + Columns.CREATED_AT + " timestamp, " + Columns.UPDATED_AT + " timestamp,"
            + " primary key (id))";//+ " primary key ((id), updated_at))"                
    //+ " with clustering order by (updated_at DESC);";
    private static final String DROP_DOC_TABLE_CQL = "drop table if exists %s;";

    private PreparedStatement existStmt;
    private PreparedStatement readStmt;
    private PreparedStatement createStmt;
    private PreparedStatement deleteStmt;
    private PreparedStatement updateStmt;
    private PreparedStatement readAllStmt;
    private PreparedStatement readAllCountStmt;
    private PreparedStatement readCountTableSizeStmt;

    public TableRepository(Session session)
    {
        super(session, Tables.BY_ID);
        addObserver(new DefaultTimestampedIdentifiableRepositoryObserver<Table>());
        addObserver(new StateChangeEventingObserver<Table>(new CollectionEventFactory()));
        initialize();
    }

    protected void initialize()
    {
        existStmt = PreparedStatementFactory.getPreparedStatement(String.format(EXISTENCE_CQL, getTable()), getSession());
        readStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_CQL, getTable()), getSession());
        createStmt = PreparedStatementFactory.getPreparedStatement(String.format(CREATE_CQL, getTable(), Columns.NAME), getSession());
        deleteStmt = PreparedStatementFactory.getPreparedStatement(String.format(DELETE_CQL, getTable()), getSession());
        updateStmt = PreparedStatementFactory.getPreparedStatement(String.format(UPDATE_CQL, getTable()), getSession());
        readAllStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_ALL_CQL, getTable()), getSession());
        readAllCountStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_ALL_COUNT_CQL, getTable()), getSession());
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
    protected Table readEntityById(Identifier identifier)
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
    protected Table createEntity(Table entity)
    {
        // Create the actual table for the documents.
        Statement s = new SimpleStatement(String.format(CREATE_DOC_TABLE_CQL, entity.toDbTable()));
        getSession().execute(s);

        // Create the metadata for the table.
        BoundStatement bs = new BoundStatement(createStmt);
        bindCreate(bs, entity);
        getSession().execute(bs);
        return entity;
    }

    @Override
    protected Table updateEntity(Table entity)
    {
        BoundStatement bs = new BoundStatement(updateStmt);
        bindUpdate(bs, entity);
        getSession().execute(bs);
        return entity;
    }

    @Override
    protected void deleteEntity(Table entity)
    {
        // Delete the actual table for the documents.
        Statement s = new SimpleStatement(String.format(DROP_DOC_TABLE_CQL, entity.toDbTable()));
        getSession().execute(s);

        BoundStatement bs = new BoundStatement(deleteStmt);
        bindIdentifier(bs, entity.getId());
        getSession().execute(bs);
    }

    public List<Table> readAll(String namespace)
    {
        BoundStatement bs = new BoundStatement(readAllStmt);
        bs.bind(namespace);
        return (marshalAll(getSession().execute(bs)));
    }

    public long countAllTables(String namespace)
    {
        BoundStatement bs = new BoundStatement(readAllCountStmt);
        bs.bind(namespace);
        return (getSession().execute(bs).one().getLong(0));
    }

    public long countTableSize(String namespace, String tableName)
    {
        readCountTableSizeStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_COUNT_TABLE_SIZE_CQL, namespace + "_" + tableName), getSession());
        BoundStatement bs = new BoundStatement(readAllCountStmt);
        bs.bind(namespace);
        return (getSession().execute(bs).one().getLong(0));
    }

    private void bindCreate(BoundStatement bs, Table entity)
    {
        bs.bind(entity.name(),
                entity.database().name(),
                entity.description(),
                entity.getCreatedAt(),
                entity.getUpdatedAt());
    }

    private void bindUpdate(BoundStatement bs, Table entity)
    {
        bs.bind(entity.description(),
                entity.getUpdatedAt(),
                entity.database().name(),
                entity.name());
    }

    private List<Table> marshalAll(ResultSet rs)
    {
        List<Table> collections = new ArrayList<>();
        Iterator<Row> i = rs.iterator();

        while (i.hasNext())
        {
            collections.add(marshalRow(i.next()));
        }

        return collections;
    }

    protected Table marshalRow(Row row)
    {
        if (row == null)
        {
            return null;
        }

        Table c = new Table();
        c.name(row.getString(Columns.NAME));
        c.database(row.getString(Columns.DATABASE));
        c.description(row.getString(Columns.DESCRIPTION));
        c.setCreatedAt(row.getDate(Columns.CREATED_AT));
        c.setUpdatedAt(row.getDate(Columns.UPDATED_AT));
        return c;
    }

    private class CollectionEventFactory
            implements EventFactory<Table>
    {

        @Override
        public Object newCreatedEvent(Table object)
        {
            return new TableCreatedEvent(object);
        }

        @Override
        public Object newUpdatedEvent(Table object)
        {
            return new TableUpdatedEvent(object);
        }

        @Override
        public Object newDeletedEvent(Table object)
        {
            return new TableDeletedEvent(object);
        }
    }
}

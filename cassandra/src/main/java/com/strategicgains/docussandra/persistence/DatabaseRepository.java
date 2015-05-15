package com.strategicgains.docussandra.persistence;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Identifier;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.persistence.parent.AbstractCassandraRepository;
import com.strategicgains.docussandra.persistence.helper.PreparedStatementFactory;
import com.strategicgains.docussandra.persistence.parent.RepositoryInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseRepository extends AbstractCassandraRepository implements RepositoryInterface<Database>
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private class Tables
    {

        static final String BY_ID = "sys_db";
    }

    private class Columns
    {

        static final String NAME = "db_name";
        static final String DESCRIPTION = "description";
        static final String CREATED_AT = "created_at";
        static final String UPDATED_AT = "updated_at";
    }

    private static final String CREATE_CQL = "insert into %s (%s, description, created_at, updated_at) values (?, ?, ?, ?)";
    private static final String UPDATE_CQL = "update %s set description = ?, updated_at = ? where %s = ?";
    private static final String READ_ALL_CQL = "select * from %s";
    private static final String EXISTENCE_CQL = "select count(*) from %s where %s = ?";
    private static final String READ_CQL = "select * from %s where %s = ?";
    private static final String DELETE_CQL = "delete from %s where %s = ?";
    //private static final String READ_ALL_CQL_WITH_LIMIT = "select * from %s LIMIT %s";

    private PreparedStatement createStmt;
    private PreparedStatement updateStmt;
    private PreparedStatement readAllStmt;
    private PreparedStatement existStmt;
    private PreparedStatement readStmt;
    protected PreparedStatement deleteStmt;

    private TableRepository tableRepo;

    public DatabaseRepository(Session session)
    {
        super(session, Tables.BY_ID);
        initializeStatements();
        tableRepo = new TableRepository(getSession());
    }

    protected void initializeStatements()
    {
        createStmt = PreparedStatementFactory.getPreparedStatement(String.format(CREATE_CQL, getTable(), Columns.NAME), getSession());
        updateStmt = PreparedStatementFactory.getPreparedStatement(String.format(UPDATE_CQL, getTable(), Columns.NAME), getSession());
        readAllStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_ALL_CQL, getTable()), getSession());
        existStmt = PreparedStatementFactory.getPreparedStatement(String.format(EXISTENCE_CQL, getTable(), Columns.NAME), getSession());
        readStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_CQL, getTable(), Columns.NAME), getSession());
        deleteStmt = PreparedStatementFactory.getPreparedStatement(String.format(DELETE_CQL, getTable(), Columns.NAME), getSession());
    }

    @Override
    public Database create(Database entity)
    {
        BoundStatement bs = new BoundStatement(createStmt);
        bindCreate(bs, entity);
        getSession().execute(bs);
        return entity;
    }

    @Override
    public Database update(Database entity)
    {
        BoundStatement bs = new BoundStatement(updateStmt);
        bindUpdate(bs, entity);
        getSession().execute(bs);
        return entity;
    }

    @Override
    public List<Database> readAll()
    {
        BoundStatement bs = new BoundStatement(readAllStmt);
        return marshalAll(getSession().execute(bs));
    }

    @Override
    public List<Database> readAll(Identifier id)
    {
        throw new UnsupportedOperationException("Call not valid for this class.");
    }

    @Override
    public boolean exists(Identifier identifier)
    {
        if (identifier == null || identifier.isEmpty())
        {
            return false;
        }

        BoundStatement bs = new BoundStatement(existStmt);
        bs.bind(identifier.getDatabaseName());
        return (getSession().execute(bs).one().getLong(0) > 0);
    }

    @Override
    public Database read(Identifier identifier)
    {
        if (identifier == null || identifier.isEmpty())
        {
            return null;
        }

        BoundStatement bs = new BoundStatement(readStmt);
        bs.bind(identifier.getDatabaseName());
        return marshalRow(getSession().execute(bs).one());
    }

    @Override
    public void delete(Database entity)
    {
        if (entity == null)
        {
            return;
        }
        delete(entity.getId());
        cascadeDelete(entity.getId());
    }

    @Override
    public void delete(Identifier identifier)
    {
        if (identifier == null)
        {
            return;
        }

        BoundStatement bs = new BoundStatement(deleteStmt);
        bindIdentifier(bs, identifier);
        getSession().execute(bs);
        cascadeDelete(identifier);
    }

    private void cascadeDelete(Identifier id)
    {
        //remove all the tables and all the documents in that database.
        //TODO: version instead of delete
        //tables
        logger.info("Cleaning up tables for database: " + id.getDatabaseName());

        List<Table> tables = tableRepo.readAll(id);//get all tables
        for (Table t : tables)
        {
            tableRepo.delete(t);// then delete them
        }
    }

    private void bindCreate(BoundStatement bs, Database entity)
    {
        bs.bind(entity.name(),
                entity.description(),
                entity.getCreatedAt(),
                entity.getUpdatedAt());
    }

    private void bindUpdate(BoundStatement bs, Database entity)
    {
        bs.bind(entity.description(),
                entity.getUpdatedAt(),
                entity.name());
    }

    private List<Database> marshalAll(ResultSet rs)
    {
        List<Database> databases = new ArrayList<>();
        Iterator<Row> i = rs.iterator();

        while (i.hasNext())
        {
            databases.add(marshalRow(i.next()));
        }

        return databases;
    }

    //@Override
    protected Database marshalRow(Row row)
    {
        if (row == null)
        {
            return null;
        }

        Database n = new Database();
        n.name(row.getString(Columns.NAME));
        n.description(row.getString(Columns.DESCRIPTION));
        n.setCreatedAt(row.getDate(Columns.CREATED_AT));
        n.setUpdatedAt(row.getDate(Columns.UPDATED_AT));
        return n;
    }
}

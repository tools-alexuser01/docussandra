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
import com.strategicgains.docussandra.domain.abstractparent.AbstractCassandraEntityRepository;
import com.strategicgains.docussandra.persistence.helper.PreparedStatementFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseRepository extends AbstractCassandraEntityRepository<Database>
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
    //private static final String DELETE_CQL = "delete from %s where %s = ?";
    //private static final String READ_ALL_CQL_WITH_LIMIT = "select * from %s LIMIT %s";

    private PreparedStatement createStmt;
    private PreparedStatement updateStmt;
    private PreparedStatement readAllStmt;
    //protected PreparedStatement deleteStmt;

    private TableRepository tableRepo;

    public DatabaseRepository(Session session)
    {
        super(session, Tables.BY_ID, Columns.NAME);
        initializeStatements();
        tableRepo = new TableRepository(getSession());
    }

//    @Override
//    protected void initializeObservers()
//    {
//        super.initializeObservers();
//        addObserver(new StateChangeEventingObserver<Database>(new NamespaceEventFactory()));
//    }
    protected void initializeStatements()
    {
        createStmt = PreparedStatementFactory.getPreparedStatement(String.format(CREATE_CQL, getTable(), getIdentifierColumn()), getSession());
        updateStmt = PreparedStatementFactory.getPreparedStatement(String.format(UPDATE_CQL, getTable(), getIdentifierColumn()), getSession());
        readAllStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_ALL_CQL, getTable()), getSession());
    }

    //@Override
    public Database createEntity(Database entity)
    {
        BoundStatement bs = new BoundStatement(createStmt);
        bindCreate(bs, entity);
        getSession().execute(bs);
        return entity;
    }

    //@Override
    public Database updateEntity(Database entity)
    {
        BoundStatement bs = new BoundStatement(updateStmt);
        bindUpdate(bs, entity);
        getSession().execute(bs);
        return entity;
    }

    @Override
    public void deleteEntity(Database entity)
    {
        super.deleteEntity(entity);
        cascadeDelete(entity.name());
    }

    @Override
    public void deleteEntityById(Identifier identifier)
    {
        super.deleteEntityById(identifier);
        cascadeDelete(identifier.components().get(0).toString());
    }

    private void cascadeDelete(String dbName)
    {
        //remove all the collections and all the documents in that database.
        //TODO: version instead of delete
        //tables
        logger.info("Cleaning up tables for database: " + dbName);

        List<Table> tables = tableRepo.readAll(dbName);//get all tables
        for (Table t : tables)
        {
            tableRepo.deleteEntity(t);// then delete them
        }
    }

    public List<Database> readAll()
    {
        BoundStatement bs = new BoundStatement(readAllStmt);
        return marshalAll(getSession().execute(bs));
    }

//    public List<Database> readAll(int limit, int offset)
//    {
//        PreparedStatement readAllStmtWithLimit = getSession().prepare(String.format(READ_ALL_CQL_WITH_LIMIT, getTable(), limit));    
//        //^^TODO: EhCache this!
//        BoundStatement bs = new BoundStatement(readAllStmtWithLimit);
//        return marshalAll(getSession().execute(bs));
//    }
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
        List<Database> namespaces = new ArrayList<Database>();
        Iterator<Row> i = rs.iterator();

        while (i.hasNext())
        {
            namespaces.add(marshalRow(i.next()));
        }

        return namespaces;
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

//    private class NamespaceEventFactory
//            implements EventFactory<Database>
//    {
//
//        @Override
//        public Object newCreatedEvent(Database object)
//        {
//            return new DatabaseCreatedEvent(object);
//        }
//
//        @Override
//        public Object newUpdatedEvent(Database object)
//        {
//            return new DatabaseUpdatedEvent(object);
//        }
//
//        @Override
//        public Object newDeletedEvent(Database object)
//        {
//            return new DatabaseDeletedEvent(object);
//        }
//    }
}

package com.strategicgains.docussandra.persistence;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.Metadata;
import com.strategicgains.docussandra.persistence.abstractparent.AbstractCassandraRepository;
import com.strategicgains.docussandra.persistence.helper.PreparedStatementFactory;

public class MetadataRepository extends AbstractCassandraRepository
{

    private class Tables
    {

        static final String BY_ID = "sys_meta";
    }

    private class Columns
    {

        static final String ID = "id";
        static final String VERSION = "version";
        static final String CREATED_AT = "created_at";
        static final String UPDATED_AT = "updated_at";
    }

    private static final String CREATE_CQL = "insert into %s (%s, version, created_at, updated_at) values (?, ?, ?, ?)";
    private static final String READ_CQL = "select * from %s limit(1)";
    private static final String READ_ALL_CQL = "select * from %s";
    private static final String DELETE_CQL = "delete from %s where %s = ?";

    private PreparedStatement createStmt;
    private PreparedStatement readAllStmt;
    protected PreparedStatement deleteStmt;

    public MetadataRepository(Session session)
    {
        super(session, Tables.BY_ID, Columns.ID);
        initializeStatements();
    }

//	@Override
//	protected void initializeObservers()
//	{
//		super.initializeObservers();
////		addObserver(new StateChangeEventingObserver<Metadata>(new MetadataEventFactory()));
//	}
    protected void initializeStatements()
    {
        createStmt = PreparedStatementFactory.getPreparedStatement(String.format(CREATE_CQL, getTable(), getIdentifierColumn()), getSession());
        readAllStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_ALL_CQL, getTable()), getSession());
        deleteStmt = PreparedStatementFactory.getPreparedStatement(String.format(DELETE_CQL, getTable(), getIdentifierColumn()), getSession());
    }

    //@Override
    protected Metadata createEntity(Metadata entity)
    {
        BoundStatement bs = new BoundStatement(createStmt);
        bindCreate(bs, entity);
        getSession().execute(bs);
        return entity;
    }

    //@Override
    protected Metadata updateEntity(Metadata entity)
    {
        return createEntity(entity);
    }

    //@Override
    protected void deleteEntity(Metadata entity)
    {
        BoundStatement bs = new BoundStatement(deleteStmt);
        bindIdentifier(bs, entity.getId());
        getSession().execute(bs);
    }

    public List<Metadata> readAll()
    {
        BoundStatement bs = new BoundStatement(readAllStmt);
        return marshalAll(getSession().execute(bs));
    }

    private void bindCreate(BoundStatement bs, Metadata entity)
    {
        bs.bind(entity.id(),
                entity.version(),
                entity.getCreatedAt(),
                entity.getUpdatedAt());
    }

    private List<Metadata> marshalAll(ResultSet rs)
    {
        List<Metadata> namespaces = new ArrayList<Metadata>();
        Iterator<Row> i = rs.iterator();

        while (i.hasNext())
        {
            namespaces.add(marshalRow(i.next()));
        }

        return namespaces;
    }

    //@Override
    protected Metadata marshalRow(Row row)
    {
        if (row == null)
        {
            return null;
        }

        Metadata n = new Metadata();
        n.id(row.getString(Columns.ID));
        n.version(row.getString(Columns.VERSION));
        n.setCreatedAt(row.getDate(Columns.CREATED_AT));
        n.setUpdatedAt(row.getDate(Columns.UPDATED_AT));
        return n;
    }

//	private class MetadataEventFactory
//	implements EventFactory<Metadata>
//	{
//		@Override
//        public Object newCreatedEvent(Metadata object)
//        {
//	        return new MetadataCreatedEvent(object);
//        }
//
//		@Override
//        public Object newUpdatedEvent(Metadata object)
//        {
//	        return new MetadataUpdatedEvent(object);
//        }
//
//		@Override
//        public Object newDeletedEvent(Metadata object)
//        {
//	        return new MetadataDeletedEvent(object);
//        }
//	}
}

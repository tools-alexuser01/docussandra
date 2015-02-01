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
import com.strategicgains.docussandra.event.DatabaseCreatedEvent;
import com.strategicgains.docussandra.event.DatabaseDeletedEvent;
import com.strategicgains.docussandra.event.DatabaseUpdatedEvent;
import com.strategicgains.docussandra.event.EventFactory;
import com.strategicgains.repoexpress.cassandra.CassandraTimestampedEntityRepository;

public class DatabaseRepository
extends CassandraTimestampedEntityRepository<Database>
{
	
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

	private PreparedStatement createStmt;
	private PreparedStatement updateStmt;
	private PreparedStatement readAllStmt;

	public DatabaseRepository(Session session)
    {
	    super(session, Tables.BY_ID, Columns.NAME);
	    initializeStatements();
    }

	@Override
	protected void initializeObservers()
	{
		super.initializeObservers();
		addObserver(new StateChangeEventingObserver<Database>(new NamespaceEventFactory()));
	}

	protected void initializeStatements()
	{
		createStmt = getSession().prepare(String.format(CREATE_CQL, getTable(), getIdentifierColumn()));
		updateStmt = getSession().prepare(String.format(UPDATE_CQL, getTable(), getIdentifierColumn()));
		readAllStmt = getSession().prepare(String.format(READ_ALL_CQL, getTable()));
	}

	@Override
    protected Database createEntity(Database entity)
    {
		BoundStatement bs = new BoundStatement(createStmt);
		bindCreate(bs, entity);
		getSession().execute(bs);
		return entity;
    }

	@Override
    protected Database updateEntity(Database entity)
    {
		BoundStatement bs = new BoundStatement(updateStmt);
		bindUpdate(bs, entity);
		getSession().execute(bs);
		return entity;
    }

	@Override
	protected void deleteEntity(Database entity)
	{
		BoundStatement bs = new BoundStatement(deleteStmt);
		bindIdentifier(bs, entity.getId());
		getSession().execute(bs);
	}

	public List<Database> readAll()
    {
		BoundStatement bs = new BoundStatement(readAllStmt);
	    return marshalAll(getSession().execute(bs));
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
		List<Database> namespaces = new ArrayList<Database>();
		Iterator<Row> i = rs.iterator();
		
		while (i.hasNext())
		{
			namespaces.add(marshalRow(i.next()));
		}

		return namespaces;
	}

	@Override
    protected Database marshalRow(Row row)
    {
		if (row == null) return null;

		Database n = new Database();
		n.name(row.getString(Columns.NAME));
		n.description(row.getString(Columns.DESCRIPTION));
		n.setCreatedAt(row.getDate(Columns.CREATED_AT));
		n.setUpdatedAt(row.getDate(Columns.UPDATED_AT));
		return n;
    }

	private class NamespaceEventFactory
	implements EventFactory<Database>
	{
		@Override
        public Object newCreatedEvent(Database object)
        {
	        return new DatabaseCreatedEvent(object);
        }

		@Override
        public Object newUpdatedEvent(Database object)
        {
	        return new DatabaseUpdatedEvent(object);
        }

		@Override
        public Object newDeletedEvent(Database object)
        {
	        return new DatabaseDeletedEvent(object);
        }
	}
}

package com.strategicgains.docussandra.persistence;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.Collection;
import com.strategicgains.docussandra.event.CollectionCreatedEvent;
import com.strategicgains.docussandra.event.CollectionDeletedEvent;
import com.strategicgains.docussandra.event.CollectionUpdatedEvent;
import com.strategicgains.docussandra.event.EventFactory;
import com.strategicgains.repoexpress.cassandra.AbstractCassandraRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.event.DefaultTimestampedIdentifiableRepositoryObserver;

public class CollectionsRepository
extends AbstractCassandraRepository<Collection>
{
	private class Tables
	{
		static final String BY_ID = "collections";
	}

	private class Columns
	{
		static final String NAME = "name";
		static final String NAMESPACE = "namespace";
		static final String DESCRIPTION = "description";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	private static final String IDENTITY_CQL = " where namespace = ? and name = ?";
	private static final String EXISTENCE_CQL = "select count(*) from %s" + IDENTITY_CQL;
	private static final String CREATE_CQL = "insert into %s (%s, namespace, description, created_at, updated_at) values (?, ?, ?, ?, ?)";
	private static final String READ_CQL = "select * from %s" + IDENTITY_CQL;
	private static final String DELETE_CQL = "delete from %s" + IDENTITY_CQL;
	private static final String UPDATE_CQL = "update %s set description = ?, updated_at = ?" + IDENTITY_CQL;
	private static final String READ_ALL_CQL = "select * from %s where namespace = ?";
	private static final String READ_ALL_COUNT_CQL = "select count(*) from %s where namespace = ?";

	private PreparedStatement existStmt;
	private PreparedStatement readStmt;
	private PreparedStatement createStmt;
	private PreparedStatement deleteStmt;
	private PreparedStatement updateStmt;
	private PreparedStatement readAllStmt;
	private PreparedStatement readAllCountStmt;

	public CollectionsRepository(Session session)
	{
		super(session, Tables.BY_ID);
		addObserver(new DefaultTimestampedIdentifiableRepositoryObserver<Collection>());
		addObserver(new StateChangeEventingObserver<Collection>(new CollectionEventFactory()));
		initialize();
	}

	protected void initialize()
	{
		existStmt = getSession().prepare(String.format(EXISTENCE_CQL, getTable()));
		readStmt = getSession().prepare(String.format(READ_CQL, getTable()));
		createStmt = getSession().prepare(String.format(CREATE_CQL, getTable(), Columns.NAME));
		deleteStmt = getSession().prepare(String.format(DELETE_CQL, getTable()));
		updateStmt = getSession().prepare(String.format(UPDATE_CQL, getTable()));
		readAllStmt = getSession().prepare(String.format(READ_ALL_CQL, getTable()));
		readAllCountStmt = getSession().prepare(String.format(READ_ALL_COUNT_CQL, getTable()));
	}

	@Override
	public boolean exists(Identifier identifier)
	{
		if (identifier == null || identifier.isEmpty()) return false;

		BoundStatement bs = new BoundStatement(existStmt);
		bindIdentifier(bs, identifier);
		return (getSession().execute(bs).one().getLong(0) > 0);
	}

	protected Collection readEntityById(Identifier identifier)
	{
		if (identifier == null || identifier.isEmpty()) return null;

		BoundStatement bs = new BoundStatement(readStmt);
		bindIdentifier(bs, identifier);
		return marshalRow(getSession().execute(bs).one());
	}

	@Override
	protected Collection createEntity(Collection entity)
	{
		BoundStatement bs = new BoundStatement(createStmt);
		bindCreate(bs, entity);
		getSession().execute(bs);
		return entity;
	}

	@Override
	protected Collection updateEntity(Collection entity)
	{
		BoundStatement bs = new BoundStatement(updateStmt);
		bindUpdate(bs, entity);
		getSession().execute(bs);
		return entity;
	}

	@Override
	protected void deleteEntity(Collection entity)
	{
		BoundStatement bs = new BoundStatement(deleteStmt);
		bindIdentifier(bs, entity.getId());
		getSession().execute(bs);
	}

	public List<Collection> readAll(String namespace)
	{
		BoundStatement bs = new BoundStatement(readAllStmt);
		bs.bind(namespace);
		return (marshalAll(getSession().execute(bs)));
	}

	public long countAll(String namespace)
	{
		BoundStatement bs = new BoundStatement(readAllCountStmt);
		bs.bind(namespace);
		return (getSession().execute(bs).one().getLong(0));
	}

	private void bindCreate(BoundStatement bs, Collection entity)
	{
		bs.bind(entity.getName(),
			entity.getNamespace(),
			entity.getDescription(),
		    entity.getCreatedAt(),
		    entity.getUpdatedAt());
	}

	private void bindUpdate(BoundStatement bs, Collection entity)
	{
		bs.bind(entity.getDescription(),
			entity.getUpdatedAt(),
			entity.getNamespace(),
			entity.getName());
	}

	private List<Collection> marshalAll(ResultSet rs)
	{
		List<Collection> collections = new ArrayList<Collection>();
		Iterator<Row> i = rs.iterator();
		
		while (i.hasNext())
		{
			collections.add(marshalRow(i.next()));
		}

		return collections;
	}

    protected Collection marshalRow(Row row)
    {
		if (row == null) return null;

		Collection c = new Collection();
		c.setName(row.getString(Columns.NAME));
		c.setNamespace(row.getString(Columns.NAMESPACE));
		c.setDescription(row.getString(Columns.DESCRIPTION));
		c.setCreatedAt(row.getDate(Columns.CREATED_AT));
		c.setUpdatedAt(row.getDate(Columns.UPDATED_AT));
		return c;
    }

	private class CollectionEventFactory
	implements EventFactory<Collection>
	{
		@Override
        public Object newCreatedEvent(Collection object)
        {
	        return new CollectionCreatedEvent(object);
        }

		@Override
        public Object newUpdatedEvent(Collection object)
        {
	        return new CollectionUpdatedEvent(object);
        }

		@Override
        public Object newDeletedEvent(Collection object)
        {
	        return new CollectionDeletedEvent(object);
        }
	}
}

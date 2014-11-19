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
import com.strategicgains.docussandra.event.EventFactory;
import com.strategicgains.docussandra.event.IndexCreatedEvent;
import com.strategicgains.docussandra.event.IndexDeletedEvent;
import com.strategicgains.docussandra.event.IndexUpdatedEvent;
import com.strategicgains.repoexpress.cassandra.AbstractCassandraRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.event.DefaultTimestampedIdentifiableRepositoryObserver;

public class IndexRepository
extends AbstractCassandraRepository<Index>
{
	private class Tables
	{
		static final String BY_ID = "doc_idx";
	}

	private class Columns
	{
		static final String NAME = "name";
		static final String NAMESPACE = "namespace";
		static final String COLLECTION = "collection";
		static final String IS_UNIQUE = "is_unique";
		static final String BUCKET_SIZE = "bucket_size";
		static final String PROPERTIES = "properties";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	private static final String IDENTITY_CQL = " where namespace = ? and collection = ? and name = ?";
	private static final String EXISTENCE_CQL = "select count(*) from %s" + IDENTITY_CQL;
	private static final String CREATE_CQL = "insert into %s (%s, namespace, collection, is_unique, bucket_size, properties, created_at, updated_at) values (?, ?, ?, ?, ?, ? , ?, ?)";
	private static final String READ_CQL = "select * from %s" + IDENTITY_CQL;
	private static final String DELETE_CQL = "delete from %s" + IDENTITY_CQL;
	private static final String UPDATE_CQL = "update %s set bucket_size = ?, updated_at = ?" + IDENTITY_CQL;
	private static final String READ_ALL_CQL = "select * from %s where namespace = ? and collection = ?";
	private static final String READ_ALL_COUNT_CQL = "select count(*) from %s where namespace = ? and collection = ?";

	private PreparedStatement existStmt;
	private PreparedStatement readStmt;
	private PreparedStatement createStmt;
	private PreparedStatement deleteStmt;
	private PreparedStatement updateStmt;
	private PreparedStatement readAllStmt;
	private PreparedStatement readAllCountStmt;

	public IndexRepository(Session session)
	{
		super(session, Tables.BY_ID);
		addObserver(new DefaultTimestampedIdentifiableRepositoryObserver<Index>());
		addObserver(new StateChangeEventingObserver<Index>(new IndexEventFactory()));
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

	protected Index readEntityById(Identifier identifier)
	{
		if (identifier == null || identifier.isEmpty()) return null;

		BoundStatement bs = new BoundStatement(readStmt);
		bindIdentifier(bs, identifier);
		return marshalRow(getSession().execute(bs).one());
	}

	@Override
	protected Index createEntity(Index entity)
	{
		BoundStatement bs = new BoundStatement(createStmt);
		bindCreate(bs, entity);
		getSession().execute(bs);
		return entity;
	}

	@Override
	protected Index updateEntity(Index entity)
	{
		BoundStatement bs = new BoundStatement(updateStmt);
		bindUpdate(bs, entity);
		getSession().execute(bs);
		return entity;
	}

	@Override
	protected void deleteEntity(Index entity)
	{
		BoundStatement bs = new BoundStatement(deleteStmt);
		bindIdentifier(bs, entity.getId());
		getSession().execute(bs);
	}

	public List<Index> readAll(String namespace, String collection)
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

	private void bindCreate(BoundStatement bs, Index entity)
	{
		bs.bind(entity.getName(),
			entity.getNamespace(),
			entity.getCollection(),
			entity.isUnique(),
			entity.getBucketSize(),
			entity.getProperties(),
		    entity.getCreatedAt(),
		    entity.getUpdatedAt());
	}

	// 	"update %s set bucket_size = ?, updated_at = ?" + IDENTITY_CQL;
	private void bindUpdate(BoundStatement bs, Index entity)
	{
		bs.bind(entity.getBucketSize(),
			entity.getUpdatedAt(),
			entity.getNamespace(),
			entity.getCollection(),
			entity.getName());
	}

	private List<Index> marshalAll(ResultSet rs)
	{
		List<Index> indexes = new ArrayList<Index>();
		Iterator<Row> i = rs.iterator();
		
		while (i.hasNext())
		{
			indexes.add(marshalRow(i.next()));
		}

		return indexes;
	}

    protected Index marshalRow(Row row)
    {
		if (row == null) return null;

		Index i = new Index();
		i.setName(row.getString(Columns.NAME));
		i.setNamespace(row.getString(Columns.NAMESPACE));
		i.setCollection(row.getString(Columns.COLLECTION));
		i.setUnique(row.getBool(Columns.IS_UNIQUE));
		i.setBucketSize(row.getLong(Columns.BUCKET_SIZE));
		i.setProperties(row.getMap(Columns.PROPERTIES, String.class, Index.IndexProperties.class));
		i.setCreatedAt(row.getDate(Columns.CREATED_AT));
		i.setUpdatedAt(row.getDate(Columns.UPDATED_AT));
		return i;
    }

	private class IndexEventFactory
	implements EventFactory<Index>
	{
		@Override
        public Object newCreatedEvent(Index object)
        {
	        return new IndexCreatedEvent(object);
        }

		@Override
        public Object newUpdatedEvent(Index object)
        {
	        return new IndexUpdatedEvent(object);
        }

		@Override
        public Object newDeletedEvent(Index object)
        {
	        return new IndexDeletedEvent(object);
        }
	}
}

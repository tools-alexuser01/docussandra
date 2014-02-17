package com.strategicgains.mongossandra.persistence;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.strategicgains.mongossandra.domain.Collection;
import com.strategicgains.repoexpress.cassandra.AbstractCassandraRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.event.DefaultTimestampedIdentifiableRepositoryObserver;
import com.strategicgains.repoexpress.event.UuidIdentityRepositoryObserver;
import com.strategicgains.repoexpress.exception.DuplicateItemException;
import com.strategicgains.syntaxe.ValidationException;

public class CollectionsRepository
extends AbstractCassandraRepository<Collection>
{
	private class Tables
	{
		static final String BY_ID = "collections";
		static final String BY_NAME = "collections_name";
	}

	private class Columns
	{
		static final String ID = "id";
		static final String NAME = "name";
		static final String NAMESPACE_ID = "namespace_id";
		static final String DESCRIPTION = "description";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	private static final String IDENTITY_CQL = " where id = ?";
	private static final String EXISTENCE_CQL = "select count(*) from %s" + IDENTITY_CQL;
	private static final String CREATE_CQL = "insert into %s (%s, name, namespace_id, description, created_at, updated_at) values (?, ?, ?, ?, ?, ?)";
	private static final String READ_CQL = "select * from %s" + IDENTITY_CQL;
	private static final String DELETE_CQL = "delete from %s" + IDENTITY_CQL;
	private static final String UPDATE_CQL = "update %s set name = ?, description = ?, updated_at = ?" + IDENTITY_CQL;
	private static final String DELETE_CQL2 = "delete from %s where namespace_id = ? and name = ?";
	private static final String READ_NAME_CQL = "select * from %s where namespace_id = ? and name = ?";
	private static final String NAME_EXISTS_CQL = "select count(*) from %s where namespace_id = ? and name = ?";
	private static final String READ_ALL_CQL = "select * from %s where namespace_id = ?";
	private static final String READ_ALL_COUNT_CQL = "select count(*) from %s where namespace_id = ?";

	private PreparedStatement existStmt;
	private PreparedStatement readStmt;
	private PreparedStatement createStmt;
	private PreparedStatement deleteStmt;
	private PreparedStatement updateStmt;
	private PreparedStatement readAllStmt;
	private PreparedStatement readAllCountStmt;
	private PreparedStatement createStmt2;
	private PreparedStatement deleteStmt2;
	private PreparedStatement readNameStmt;
	private PreparedStatement nameExistsStmt;

	public CollectionsRepository(Session session)
	{
		super(session, Tables.BY_ID);
		addObserver(new DefaultTimestampedIdentifiableRepositoryObserver<Collection>());
		addObserver(new UuidIdentityRepositoryObserver<Collection>());
		initialize();
	}

	protected void initialize()
	{
		existStmt = getSession().prepare(String.format(EXISTENCE_CQL, getTable()));
		readStmt = getSession().prepare(String.format(READ_CQL, getTable()));
		createStmt = getSession().prepare(String.format(CREATE_CQL, getTable(), Columns.ID));
		createStmt2 = getSession().prepare(String.format(CREATE_CQL, Tables.BY_NAME, Columns.ID));
		deleteStmt = getSession().prepare(String.format(DELETE_CQL, getTable()));
		updateStmt = getSession().prepare(String.format(UPDATE_CQL, getTable()));
		deleteStmt2 = getSession().prepare(String.format(DELETE_CQL2, Tables.BY_NAME));
		readNameStmt = getSession().prepare(String.format(READ_NAME_CQL, Tables.BY_NAME));
		nameExistsStmt = getSession().prepare(String.format(NAME_EXISTS_CQL, Tables.BY_NAME));
		readAllStmt = getSession().prepare(String.format(READ_ALL_CQL, Tables.BY_NAME));
		readAllCountStmt = getSession().prepare(String.format(READ_ALL_COUNT_CQL, Tables.BY_NAME));
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
		if (nameExists(entity.getNamespaceId(), entity.getName()))
		{
			throw new DuplicateItemException("Collection already exists: " + entity.getName());
		}

		BatchStatement batch = new BatchStatement();
		BoundStatement bs = new BoundStatement(createStmt);
		bindCreate(bs, entity);
		batch.add(bs);
		
		BoundStatement bs2 = new BoundStatement(createStmt2);
		bindCreate(bs2, entity);
		batch.add(bs2);

		getSession().execute(batch);
		return entity;
	}

	@Override
	protected Collection updateEntity(Collection entity)
	{
		Collection prev = read(entity.getId());
		
		if (!prev.getName().equals(entity.getName()) && nameExists(entity.getNamespaceId(), entity.getName()))
		{
			throw new DuplicateItemException("Collection already exists: " + entity.getName());
		}

		if (!prev.getNamespaceId().equals(entity.getNamespaceId()))
		{
			throw new ValidationException("Cannot update namespace");
		}

		entity.setCreatedAt(prev.getCreatedAt());
		BatchStatement batch = new BatchStatement();
		BoundStatement bs = new BoundStatement(updateStmt);
		bindUpdate(bs, entity);
		batch.add(bs);

		// Delete the indexed-by-name instance
		BoundStatement bs2 = new BoundStatement(deleteStmt2);
		bs2.bind(prev.getName());
		batch.add(bs2);

		// Insert the new name
		BoundStatement bs3 = new BoundStatement(createStmt2);
		bindCreate(bs3, entity);
		batch.add(bs3);

		getSession().execute(batch);
		return entity;
	}

	@Override
	protected void deleteEntity(Collection entity)
	{
		BatchStatement batch = new BatchStatement();
		BoundStatement bs = new BoundStatement(deleteStmt);
		bs.bind(entity.getUuid());
		batch.add(bs);

		BoundStatement bs2 = new BoundStatement(deleteStmt2);
		bs2.bind(entity.getUuid(), entity.getName());
		batch.add(bs2);

		getSession().execute(batch);
	}

	public boolean nameExists(UUID namespaceId, String name)
	{
		if (name == null || name.isEmpty()) return false;

		BoundStatement bs = new BoundStatement(nameExistsStmt);
		bs.bind(namespaceId, name);
		return (getSession().execute(bs).one().getLong(0) > 0);
	}

	public Collection readByName(UUID namespaceId, String name)
	{
		if (name == null || name.isEmpty()) return null;
		
		BoundStatement bs = new BoundStatement(readNameStmt);
		bs.bind(namespaceId, name);
		return marshalRow(getSession().execute(bs).one());
	}

	public List<Collection> readAll(UUID namespaceId)
	{
		BoundStatement bs = new BoundStatement(readAllStmt);
		bs.bind(namespaceId);
		return (marshalAll(getSession().execute(bs)));
	}

	public long countAll(UUID namespaceId)
	{
		BoundStatement bs = new BoundStatement(readAllCountStmt);
		bs.bind(namespaceId);
		return (getSession().execute(bs).one().getLong(0));
	}

	private void bindCreate(BoundStatement bs, Collection entity)
	{
		bs.bind(entity.getUuid(),
			entity.getName(),
			entity.getNamespaceId(),
			entity.getDescription(),
		    entity.getCreatedAt(),
		    entity.getUpdatedAt());
	}

	private void bindUpdate(BoundStatement bs, Collection entity)
	{
		bs.bind(entity.getName(),
			entity.getDescription(),
			entity.getUpdatedAt(),
		    entity.getUuid());
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
		c.setUuid(row.getUUID(Columns.ID));
		c.setName(row.getString(Columns.NAME));
		c.setNamespaceId(row.getUUID(Columns.NAMESPACE_ID));
		c.setDescription(row.getString(Columns.DESCRIPTION));
		c.setCreatedAt(row.getDate(Columns.CREATED_AT));
		c.setUpdatedAt(row.getDate(Columns.UPDATED_AT));
		return c;
    }
}

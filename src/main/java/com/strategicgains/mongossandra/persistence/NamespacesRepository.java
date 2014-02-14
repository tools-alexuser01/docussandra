package com.strategicgains.mongossandra.persistence;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.strategicgains.mongossandra.domain.Namespace;
import com.strategicgains.repoexpress.cassandra.CassandraUuidTimestampedEntityRepository;
import com.strategicgains.repoexpress.exception.DuplicateItemException;

public class NamespacesRepository
extends CassandraUuidTimestampedEntityRepository<Namespace>
{
	
	private class Tables
	{
		static final String BY_ID = "namespaces";
		static final String BY_NAME = "namespaces_name";
	}

	private class Columns
	{
		static final String ID = "id";
		static final String NAME = "name";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	private static final String UPDATE_CQL = "update %s set name = ?, updated_at = ? where %s = ?";
	private static final String CREATE_CQL = "insert into %s (%s, name, created_at, updated_at) values (?, ?, ?, ?)";
	private static final String DELETE_CQL2 = "delete from %s where name = ?";
	private static final String READ_NAME_CQL = "select * from %s where name = ?";
	private static final String NAME_EXISTS_CQL = "select count(*) from %s where name = ?";
	private static final String READ_ALL_CQL = "select * from %s";

	private PreparedStatement createStmt;
	private PreparedStatement createStmt2;
	private PreparedStatement updateStmt;
	private PreparedStatement deleteStmt2;
	private PreparedStatement readNameStmt;
	private PreparedStatement nameExistsStmt;
	private PreparedStatement readAllStmt;

	public NamespacesRepository(Session session)
    {
	    super(session, Tables.BY_ID, Columns.ID);
	    initializeStatements();
    }

	protected void initializeStatements()
	{
		createStmt = getSession().prepare(String.format(CREATE_CQL, getTable(), getIdentifierColumn()));
		createStmt2 = getSession().prepare(String.format(CREATE_CQL, Tables.BY_NAME, getIdentifierColumn()));
		updateStmt = getSession().prepare(String.format(UPDATE_CQL, getTable(), getIdentifierColumn()));
		deleteStmt2 = getSession().prepare(String.format(DELETE_CQL2, Tables.BY_NAME));
		readNameStmt = getSession().prepare(String.format(READ_NAME_CQL, Tables.BY_NAME));
		nameExistsStmt = getSession().prepare(String.format(NAME_EXISTS_CQL, Tables.BY_NAME));
		readAllStmt = getSession().prepare(String.format(READ_ALL_CQL, Tables.BY_NAME));
	}

	@Override
    protected Namespace createEntity(Namespace entity)
    {
		if (nameExists(entity.getName()))
		{
			throw new DuplicateItemException("Namespace already exists: " + entity.getName());
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
    protected Namespace updateEntity(Namespace entity)
    {
		Namespace prev = read(entity.getId());
		
		if (!prev.getName().equals(entity.getName()) && nameExists(entity.getName()))
		{
			throw new DuplicateItemException("Namespace already exists: " + entity.getName());
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
	protected void deleteEntity(Namespace entity)
	{
		BatchStatement batch = new BatchStatement();
		BoundStatement bs = new BoundStatement(deleteStmt);
		bs.bind(entity.getUuid());
		batch.add(bs);

		BoundStatement bs2 = new BoundStatement(deleteStmt2);
		bs2.bind(entity.getName());
		batch.add(bs2);

		getSession().execute(batch);
	}

	public boolean nameExists(String name)
	{
		if (name == null || name.isEmpty()) return false;

		BoundStatement bs = new BoundStatement(nameExistsStmt);
		bs.bind(name);
		return (getSession().execute(bs).one().getLong(0) > 0);
	}

	public Namespace readByName(String name)
	{
		if (name == null || name.isEmpty()) return null;
		
		BoundStatement bs = new BoundStatement(readNameStmt);
		bs.bind(name);
		return marshalRow(getSession().execute(bs).one());
	}

	public List<Namespace> readAll()
    {
		BoundStatement bs = new BoundStatement(readAllStmt);
	    return marshalAll(getSession().execute(bs));
    }

	private void bindCreate(BoundStatement bs, Namespace entity)
	{
		bs.bind(entity.getUuid(),
			entity.getName(),
		    entity.getCreatedAt(),
		    entity.getUpdatedAt());
	}

	private void bindUpdate(BoundStatement bs, Namespace entity)
	{
		bs.bind(entity.getName(),
			entity.getUpdatedAt(),
		    entity.getUuid());
	}

	private List<Namespace> marshalAll(ResultSet rs)
	{
		List<Namespace> namespaces = new ArrayList<Namespace>();
		Iterator<Row> i = rs.iterator();
		
		while (i.hasNext())
		{
			namespaces.add(marshalRow(i.next()));
		}

		return namespaces;
	}

	@Override
    protected Namespace marshalRow(Row row)
    {
		if (row == null) return null;

		Namespace n = new Namespace();
		n.setUuid(row.getUUID(getIdentifierColumn()));
		n.setName(row.getString(Columns.NAME));
		n.setCreatedAt(row.getDate(Columns.CREATED_AT));
		n.setUpdatedAt(row.getDate(Columns.UPDATED_AT));
		return n;
    }
}

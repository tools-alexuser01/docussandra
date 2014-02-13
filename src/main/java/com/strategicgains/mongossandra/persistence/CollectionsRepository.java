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
import com.strategicgains.mongossandra.domain.Collection;
import com.strategicgains.repoexpress.cassandra.CassandraUuidTimestampedEntityRepository;
import com.strategicgains.repoexpress.exception.DuplicateItemException;

public class CollectionsRepository
extends CassandraUuidTimestampedEntityRepository<Collection>
{
	private static final String PRIMARY_TABLE = "collections";
	private static final String SECONDARY_TABLE = "collections_name";

	private static final String UPDATE_CQL = "update %s set name = ?, namespace = ?, description = ? updated_at = ? where %s = ?";
	private static final String CREATE_CQL = "insert into %s (%s, name, namespace, description, created_at, updated_at) values (?, ?, ?, ?, ?, ?)";
	private static final String DELETE_CQL2 = "delete from %s where name = ?";
	private static final String READ_NAME_CQL = "select * from %s where name = ?";
	private static final String NAME_EXISTS_CQL = "select count(*) from %s where namespace = ? and name = ?";
	private static final String READ_ALL_CQL = "select * from %s where namespace = ?";

	private PreparedStatement createStmt;
	private PreparedStatement createStmt2;
	private PreparedStatement updateStmt;
	private PreparedStatement deleteStmt2;
	private PreparedStatement readNameStmt;
	private PreparedStatement nameExistsStmt;
	private PreparedStatement readAllStmt;

	public CollectionsRepository(Session session)
    {
	    super(session, PRIMARY_TABLE, "id");
	    initializeStatements();
    }

	protected void initializeStatements()
	{
		createStmt = getSession().prepare(String.format(CREATE_CQL, getTable(), getIdentifierColumn()));
		createStmt2 = getSession().prepare(String.format(CREATE_CQL, SECONDARY_TABLE, getIdentifierColumn()));
		updateStmt = getSession().prepare(String.format(UPDATE_CQL, getTable(), getIdentifierColumn()));
		deleteStmt2 = getSession().prepare(String.format(DELETE_CQL2, SECONDARY_TABLE));
		readNameStmt = getSession().prepare(String.format(READ_NAME_CQL, SECONDARY_TABLE));
		nameExistsStmt = getSession().prepare(String.format(NAME_EXISTS_CQL, SECONDARY_TABLE));
		readAllStmt = getSession().prepare(String.format(READ_ALL_CQL, SECONDARY_TABLE));
	}

	@Override
    protected Collection createEntity(Collection entity)
    {
		if (nameExists(entity.getNamespace(), entity.getName()))
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
		
		if (!prev.getName().equals(entity.getName()) && nameExists(entity.getNamespace(), entity.getName()))
		{
			throw new DuplicateItemException("Collection already exists: " + entity.getName());
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
		bs2.bind(entity.getName());
		batch.add(bs2);

		getSession().execute(batch);
	}

	public boolean nameExists(String namespace, String name)
	{
		if (name == null || name.isEmpty()) return false;

		BoundStatement bs = new BoundStatement(nameExistsStmt);
		bs.bind(namespace, name);
		return (getSession().execute(bs).one().getLong(0) > 0);
	}

	public Collection readByName(String name)
	{
		if (name == null || name.isEmpty()) return null;
		
		BoundStatement bs = new BoundStatement(readNameStmt);
		bs.bind(name);
		return marshalRow(getSession().execute(bs).one());
	}

	public List<Collection> readAll(String namespace)
    {
		BoundStatement bs = new BoundStatement(readAllStmt);
		bs.bind(namespace);
	    return marshalResultSet(getSession().execute(bs));
    }

	private void bindCreate(BoundStatement bs, Collection entity)
	{
		bs.bind(entity.getUuid(),
			entity.getName(),
			entity.getNamespace(),
			entity.getDescription(),
		    entity.getCreatedAt(),
		    entity.getUpdatedAt());
	}

	private void bindUpdate(BoundStatement bs, Collection entity)
	{
		bs.bind(entity.getName(),
			entity.getNamespace(),
			entity.getDescription(),
			entity.getUpdatedAt(),
		    entity.getUuid());
	}

	private List<Collection> marshalResultSet(ResultSet rs)
	{
		List<Collection> collections = new ArrayList<Collection>();
		Iterator<Row> i = rs.iterator();
		
		while (i.hasNext())
		{
			collections.add(marshalRow(i.next()));
		}

		return collections;
	}

	@Override
    protected Collection marshalRow(Row row)
    {
		if (row == null) return null;

		Collection c = new Collection();
		c.setUuid(row.getUUID(getIdentifierColumn()));
		c.setName(row.getString("name"));
		c.setNamespace(row.getString("namespace"));
		c.setDescription(row.getString("description"));
		c.setCreatedAt(row.getDate("created_at"));
		c.setUpdatedAt(row.getDate("updated_at"));
		return c;
    }
}

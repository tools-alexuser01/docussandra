package com.strategicgains.mongossandra.persistence;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.strategicgains.mongossandra.domain.Namespace;
import com.strategicgains.repoexpress.cassandra.CassandraTimestampedEntityRepository;

public class NamespacesRepository
extends CassandraTimestampedEntityRepository<Namespace>
{
	
	private class Tables
	{
		static final String BY_ID = "namespaces";
	}

	private class Columns
	{
		static final String NAME = "name";
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

	public NamespacesRepository(Session session)
    {
	    super(session, Tables.BY_ID, Columns.NAME);
	    initializeStatements();
    }

	protected void initializeStatements()
	{
		createStmt = getSession().prepare(String.format(CREATE_CQL, getTable(), getIdentifierColumn()));
		updateStmt = getSession().prepare(String.format(UPDATE_CQL, getTable(), getIdentifierColumn()));
		readAllStmt = getSession().prepare(String.format(READ_ALL_CQL, getTable()));
	}

	@Override
    protected Namespace createEntity(Namespace entity)
    {
		BoundStatement bs = new BoundStatement(createStmt);
		bindCreate(bs, entity);
		getSession().execute(bs);
		return entity;
    }

	@Override
    protected Namespace updateEntity(Namespace entity)
    {
		BoundStatement bs = new BoundStatement(updateStmt);
		bindUpdate(bs, entity);
		getSession().execute(bs);
		return entity;
    }

	@Override
	protected void deleteEntity(Namespace entity)
	{
		BoundStatement bs = new BoundStatement(deleteStmt);
		bindIdentifier(bs, entity.getId());
		getSession().execute(bs);
	}

	public List<Namespace> readAll()
    {
		BoundStatement bs = new BoundStatement(readAllStmt);
	    return marshalAll(getSession().execute(bs));
    }

	private void bindCreate(BoundStatement bs, Namespace entity)
	{
		bs.bind(entity.getName(),
			entity.getDescription(),
		    entity.getCreatedAt(),
		    entity.getUpdatedAt());
	}

	private void bindUpdate(BoundStatement bs, Namespace entity)
	{
		bs.bind(entity.getDescription(),
			entity.getUpdatedAt(),
		    entity.getName());
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
		n.setName(row.getString(Columns.NAME));
		n.setDescription(row.getString(Columns.DESCRIPTION));
		n.setCreatedAt(row.getDate(Columns.CREATED_AT));
		n.setUpdatedAt(row.getDate(Columns.UPDATED_AT));
		return n;
    }
}

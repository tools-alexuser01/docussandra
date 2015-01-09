package com.strategicgains.docussandra.persistence;

import com.strategicgains.docussandra.domain.XXXUuidEntity;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.strategicgains.repoexpress.cassandra.CassandraTimestampedEntityRepository;

public class XXXUuidEntityRepository
extends CassandraTimestampedEntityRepository<XXXUuidEntity>
{
	private static final String UPDATE_CQL = "update %s set updatedat = ? where %s = ?";
	private static final String CREATE_CQL = "insert into %s (%s, createdat, updatedat) values (?, ?, ?)";

	private PreparedStatement createStmt;
	private PreparedStatement updateStmt;

	public XXXUuidEntityRepository(Session session)
    {
	    super(session, "sampleuuidentities", "id");
	    initializeStatements();
    }

	protected void initializeStatements()
	{
		createStmt = getSession().prepare(String.format(CREATE_CQL, getTable(), getIdentifierColumn()));
		updateStmt = getSession().prepare(String.format(UPDATE_CQL, getTable(), getIdentifierColumn()));
	}

	@Override
    protected XXXUuidEntity createEntity(XXXUuidEntity entity)
    {
		BoundStatement bs = new BoundStatement(createStmt);
		bindCreate(bs, entity);
		getSession().execute(bs);
		return entity;
    }

	@Override
    protected XXXUuidEntity updateEntity(XXXUuidEntity entity)
    {
		BoundStatement bs = new BoundStatement(updateStmt);
		bindUpdate(bs, entity);
		getSession().execute(bs);
		return entity;
    }

	private void bindCreate(BoundStatement bs, XXXUuidEntity entity)
	{
		bs.bind(entity.getUuid(),
		    entity.getCreatedAt(),
		    entity.getUpdatedAt());
	}

	private void bindUpdate(BoundStatement bs, XXXUuidEntity entity)
	{
		bs.bind(entity.getUpdatedAt(),
		    entity.getUuid());
	}

	@Override
    protected XXXUuidEntity marshalRow(Row row)
    {
		if (row == null) return null;

		XXXUuidEntity s = new XXXUuidEntity();
		s.setUuid(row.getUUID(getIdentifierColumn()));
		s.setCreatedAt(row.getDate("createdat"));
		s.setUpdatedAt(row.getDate("updatedat"));
		return s;
    }
}

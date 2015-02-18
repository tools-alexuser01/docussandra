package com.strategicgains.docussandra.persistence;

import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.Query;

public class QueryDao //extends CassandraTimestampedEntityRepository<Query>
{
//	private static final String UPDATE_CQL = "update %s set updatedat = ? where %s = ?";
//	private static final String CREATE_CQL = "insert into %s (%s, createdat, updatedat) values (?, ?, ?)";
//
//	private PreparedStatement createStmt;
//	private PreparedStatement updateStmt;
//        

    private Session session;

    public QueryDao(Session session) {
        //super(session, "queries", "id");
        this.session = session;
        //initializeStatements();
    }

    public Object doQuery(String db, Query query) {
        throw new UnsupportedOperationException("Not done yet");
    }

//	protected void initializeStatements()
//	{
//		createStmt = getSession().prepare(String.format(CREATE_CQL, getTable(), getIdentifierColumn()));
//		updateStmt = getSession().prepare(String.format(UPDATE_CQL, getTable(), getIdentifierColumn()));
//	}
//	@Override
//    protected Query createEntity(Query entity)
//    {
//		BoundStatement bs = new BoundStatement(createStmt);
//		bindCreate(bs, entity);
//		getSession().execute(bs);
//		return entity;
//    }
//
//	@Override
//    protected Query updateEntity(Query entity)
//    {
//		BoundStatement bs = new BoundStatement(updateStmt);
//		bindUpdate(bs, entity);
//		getSession().execute(bs);
//		return entity;
//    }
//
//	private void bindCreate(BoundStatement bs, Query entity)
//	{
//		bs.bind(entity.getUuid(),
//		    entity.getCreatedAt(),
//		    entity.getUpdatedAt());
//	}
//
//	private void bindUpdate(BoundStatement bs, Query entity)
//	{
//		bs.bind(entity.getUpdatedAt(),
//		    entity.getUuid());
//	}
//
//	@Override
//    protected Query marshalRow(Row row)
//    {
//		if (row == null) return null;
//
//		Query q = new Query();
//		q.setUuid(row.getUUID(getIdentifierColumn()));
//		q.setCreatedAt(row.getDate("createdat"));
//		q.setUpdatedAt(row.getDate("updatedat"));
//		return q;
//    }
}

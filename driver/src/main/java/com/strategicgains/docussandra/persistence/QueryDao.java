package com.strategicgains.docussandra.persistence;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.mongodb.util.JSON;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.ParsedQuery;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.bson.BSON;
import org.bson.BSONObject;

public class QueryDao //extends CassandraTimestampedEntityRepository<Query>
{
//	private static final String UPDATE_CQL = "update %s set updatedat = ? where %s = ?";
//	private static final String CREATE_CQL = "insert into %s (%s, createdat, updatedat) values (?, ?, ?)";
//
//	private PreparedStatement createStmt;
//	private PreparedStatement updateStmt;
//      
    private static final String QUERY_CQL = "select * from %s where %s";

    private Session session;

    public QueryDao(Session session) {
        //super(session, "queries", "id");
        this.session = session;
        //initializeStatements();
    }

    public List<Document> doQuery(String db, ParsedQuery query) {                
        //format QUERY_CQL
        String finalQuery = String.format(QUERY_CQL, query.getITable(), query.getWhereClause().getBoundStatementSyntax());
        //run query
        PreparedStatement ps = session.prepare(finalQuery);//TODO: Cache
        BoundStatement bs = new BoundStatement(ps);
        int i = 0;
        for(String bindValue : query.getWhereClause().getValues()){//no great reason for not using the other loop format
            bs.bind(i, bindValue);
            i++;
        }        
        ResultSet results = session.execute(bs);
        //process result(s)
        //right now we just are going go return a list of documents
        ArrayList<Document> toReturn = new ArrayList<>();
        Iterator<Row> ite = results.iterator();
        while(ite.hasNext()){
            Row row = ite.next();                        
            toReturn.add(DocumentRepository.marshalRow(row));
        }
        return toReturn;
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

    /**
     * @return the session
     */
    public Session getSession() {
        return session;
    }

    /**
     * @param session the session to set
     */
    public void setSession(Session session) {
        this.session = session;
    }
}

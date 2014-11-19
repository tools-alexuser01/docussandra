package com.strategicgains.docussandra.persistence;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bson.BSON;
import org.bson.BSONObject;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.mongodb.util.JSON;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.repoexpress.cassandra.CassandraUuidTimestampedEntityRepository;

public class DocumentsRepository
extends CassandraUuidTimestampedEntityRepository<Document>
{
	private class Tables
	{
		static final String BY_ID = "documents";
	}

	private class Columns
	{
		static final String ID = "id";
		static final String NAMESPACE = "namespace";
		static final String COLLECTION = "collection";
		static final String OBJECT = "object";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	private static final String UPDATE_CQL = "update %s set object = ?, updated_at = ? where %s = ?";
	private static final String CREATE_CQL = "insert into %s (%s, namespace, collection, object, created_at, updated_at) values (?, ?, ?, ?, ?, ?)";
	private static final String READ_ALL_CQL = "select * from %s where namespace = ? and collection = ? allow filtering";
	private static final String READ_ALL_COUNT_CQL = "select count(*) from %s where namespace = ? and collection = ? allow filtering";


	private PreparedStatement createStmt;
	private PreparedStatement updateStmt;
	private PreparedStatement readAllStmt;
	private PreparedStatement readAllCountStmt;

	public DocumentsRepository(Session session)
    {
	    super(session, Tables.BY_ID, Columns.ID);
	    initializeStatements();
    }

	protected void initializeStatements()
	{
		createStmt = getSession().prepare(String.format(CREATE_CQL, getTable(), getIdentifierColumn()));
		updateStmt = getSession().prepare(String.format(UPDATE_CQL, getTable(), getIdentifierColumn()));
		readAllStmt = getSession().prepare(String.format(READ_ALL_CQL, getTable(), getIdentifierColumn()));
		readAllCountStmt = getSession().prepare(String.format(READ_ALL_COUNT_CQL, getTable(), getIdentifierColumn()));
	}

	@Override
    protected Document createEntity(Document entity)
    {
		BoundStatement bs = new BoundStatement(createStmt);
		bindCreate(bs, entity);
		getSession().execute(bs);
		return entity;
    }

	@Override
    protected Document updateEntity(Document entity)
    {
		BoundStatement bs = new BoundStatement(updateStmt);
		bindUpdate(bs, entity);
		getSession().execute(bs);
		return entity;
    }

	public List<Document> readAll(String namespace, String collection)
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

	private void bindCreate(BoundStatement bs, Document entity)
	{
		BSONObject bson = (BSONObject) JSON.parse(entity.getObject());

		bs.bind(entity.getUuid(),
			entity.getNamespace(),
			entity.getCollection(),
			ByteBuffer.wrap(BSON.encode(bson)),
		    entity.getCreatedAt(),
		    entity.getUpdatedAt());
	}

	private void bindUpdate(BoundStatement bs, Document entity)
	{
		BSONObject bson = (BSONObject) JSON.parse(entity.getObject());

		bs.bind(ByteBuffer.wrap(BSON.encode(bson)),
			entity.getUpdatedAt(),
		    entity.getUuid());
	}

	private List<Document> marshalAll(ResultSet rs)
	{
		List<Document> documents = new ArrayList<Document>();
		Iterator<Row> i = rs.iterator();
		
		while (i.hasNext())
		{
			documents.add(marshalRow(i.next()));
		}

		return documents;
	}

	@Override
    protected Document marshalRow(Row row)
    {
		if (row == null) return null;

		Document d = new Document();
		d.setUuid(row.getUUID(getIdentifierColumn()));
		d.setNamespace(row.getString(Columns.NAMESPACE));
		d.setCollection(row.getString(Columns.COLLECTION));
		ByteBuffer b = row.getBytes(Columns.OBJECT);

		if (b != null && b.hasArray())
		{
			byte[] result = new byte[b.remaining()];
			b.get(result);
			BSONObject o = BSON.decode(result);

			if (!o.containsField(Columns.ID))
			{
				o.put(Columns.ID, d.getUuid().toString());
			}

			d.setObject(JSON.serialize(o));
		}

		d.setCreatedAt(row.getDate(Columns.CREATED_AT));
		d.setUpdatedAt(row.getDate(Columns.UPDATED_AT));
		return d;
    }
}

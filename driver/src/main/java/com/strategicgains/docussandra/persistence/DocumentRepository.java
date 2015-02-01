package com.strategicgains.docussandra.persistence;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BSON;
import org.bson.BSONObject;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.mongodb.util.JSON;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.event.DocumentCreatedEvent;
import com.strategicgains.docussandra.event.DocumentDeletedEvent;
import com.strategicgains.docussandra.event.DocumentUpdatedEvent;
import com.strategicgains.docussandra.event.EventFactory;
import com.strategicgains.repoexpress.cassandra.AbstractCassandraRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.event.DefaultTimestampedIdentifiableRepositoryObserver;
import com.strategicgains.repoexpress.event.UuidIdentityRepositoryObserver;

public class DocumentRepository
extends AbstractCassandraRepository<Document>
{
	private class Columns
	{
		static final String ID = "id";
		static final String OBJECT = "object";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	private static final String EXISTENCE_CQL = "select count(*) from %s where %s = ?";
	private static final String READ_CQL = "select * from %s where %s = ?";
	private static final String DELETE_CQL = "delete from %s where %s = ?";
	private static final String UPDATE_CQL = "update %s set object = ?, updated_at = ? where %s = ?";
	private static final String CREATE_CQL = "insert into %s (%s, object, created_at, updated_at) values (?, ?, ?, ?)";

	private Map<Table, PreparedStatement> createStmts = new HashMap<>();
	private Map<Table, PreparedStatement> readStmts = new HashMap<>();
	private Map<Table, PreparedStatement> existsStmts = new HashMap<>();
	private Map<Table, PreparedStatement> updateStmts = new HashMap<>();
	private Map<Table, PreparedStatement> deleteStmts = new HashMap<>();

	public DocumentRepository(Session session)
    {
	    super(session, null);
	    addObserver(new UuidIdentityRepositoryObserver<Document>());
		addObserver(new DefaultTimestampedIdentifiableRepositoryObserver<Document>());
		addObserver(new StateChangeEventingObserver<Document>(new DocumentEventFactory()));
    }

	@Override
	public boolean exists(Identifier identifier)
	{
		if (identifier == null || identifier.isEmpty()) return false;

		Table table = extractTable(identifier);
		PreparedStatement existStmt = existsStmts.get(table);

		if (existStmt == null)
		{
			existStmt = getSession().prepare(String.format(EXISTENCE_CQL, table.toDbTable(), Columns.ID));
			existsStmts.put(table, existStmt);
		}

		BoundStatement bs = new BoundStatement(existStmt);
		bs.bind(extractId(identifier));
		return (getSession().execute(bs).one().getLong(0) > 0);
	}

	@Override
	protected Document readEntityById(Identifier identifier)
	{
		if (identifier == null || identifier.isEmpty()) return null;

		Table table = extractTable(identifier);
		PreparedStatement readStmt = readStmts.get(table);

		if (readStmt == null)
		{
			readStmt = getSession().prepare(String.format(READ_CQL, table.toDbTable(), Columns.ID));
			readStmts.put(table, readStmt);
		}
		
		BoundStatement bs = new BoundStatement(readStmt);
		bs.bind(extractId(identifier));
		return marshalRow(getSession().execute(bs).one());
	}

	@Override
    protected Document createEntity(Document entity)
    {
		Table table = entity.table();
		PreparedStatement createStmt = createStmts.get(table);

		if (createStmt == null)
		{
			createStmt = getSession().prepare(String.format(CREATE_CQL, table.toDbTable(), Columns.ID));
			createStmts.put(table, createStmt);
		}

		BoundStatement bs = new BoundStatement(createStmt);
		bindCreate(bs, entity);
		getSession().execute(bs);
		return entity;
    }

	@Override
    protected Document updateEntity(Document entity)
    {
		Table table = entity.table();
		PreparedStatement updateStmt = updateStmts.get(table);

		if (updateStmt == null)
		{
			updateStmt = getSession().prepare(String.format(UPDATE_CQL, table.toDbTable(), Columns.ID));
			updateStmts.put(table, updateStmt);
		}

		BoundStatement bs = new BoundStatement(updateStmt);
		bindUpdate(bs, entity);
		getSession().execute(bs);
		return entity;
    }

	@Override
    protected void deleteEntity(Document entity)
    {
		Table table = entity.table();
		PreparedStatement deleteStmt = deleteStmts.get(table);

		if (deleteStmt == null)
		{
			deleteStmt = getSession().prepare(String.format(DELETE_CQL, table.toDbTable(), Columns.ID));
			deleteStmts.put(table, deleteStmt);
		}

		BoundStatement bs = new BoundStatement(deleteStmt);
		bindIdentifier(bs, entity.getId());
		getSession().execute(bs);
    }

	private void bindCreate(BoundStatement bs, Document entity)
	{
		BSONObject bson = (BSONObject) JSON.parse(entity.object());

		bs.bind(entity.getUuid(),
			ByteBuffer.wrap(BSON.encode(bson)),
		    entity.getCreatedAt(),
		    entity.getUpdatedAt());
	}

	private void bindUpdate(BoundStatement bs, Document entity)
	{
		BSONObject bson = (BSONObject) JSON.parse(entity.object());

		bs.bind(ByteBuffer.wrap(BSON.encode(bson)),
			entity.getUpdatedAt(),
		    entity.getUuid());
	}

	private Object[] extractId(Identifier identifier)
    {
		List<Object> l = identifier.components().subList(2, 3);
		return l.toArray();
    }

	private Table extractTable(Identifier identifier)
    {
		Table t = new Table();
		List<Object> l = identifier.components().subList(0, 2);
		t.database((String) l.get(0));
		t.name((String) l.get(1));
		return t;
    }

    protected Document marshalRow(Row row)
    {
		if (row == null) return null;

		Document d = new Document();
		d.setUuid(row.getUUID(Columns.ID));
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

			d.object(JSON.serialize(o));
		}

		d.setCreatedAt(row.getDate(Columns.CREATED_AT));
		d.setUpdatedAt(row.getDate(Columns.UPDATED_AT));
		return d;
    }

	private class DocumentEventFactory
	implements EventFactory<Document>
	{
		@Override
        public Object newCreatedEvent(Document object)
        {
	        return new DocumentCreatedEvent(object);
        }

		@Override
        public Object newUpdatedEvent(Document object)
        {
	        return new DocumentUpdatedEvent(object);
        }

		@Override
        public Object newDeletedEvent(Document object)
        {
	        return new DocumentDeletedEvent(object);
        }
	}
}

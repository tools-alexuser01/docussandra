package com.strategicgains.docussandra.persistence;

import com.datastax.driver.core.BatchStatement;
import java.nio.ByteBuffer;
import java.util.List;

import org.bson.BSON;
import org.bson.BSONObject;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.mongodb.util.JSON;
import com.strategicgains.docussandra.bucketmanagement.IndexBucketLocator;
import com.strategicgains.docussandra.bucketmanagement.SimpleIndexBucketLocatorImpl;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.QueryResponseWrapper;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.event.DocumentCreatedEvent;
import com.strategicgains.docussandra.event.DocumentDeletedEvent;
import com.strategicgains.docussandra.event.DocumentUpdatedEvent;
import com.strategicgains.docussandra.event.EventFactory;
import com.strategicgains.docussandra.handler.IndexMaintainerHelper;
import com.strategicgains.docussandra.persistence.helper.PreparedStatementFactory;
import com.strategicgains.repoexpress.AbstractObservableRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.event.DefaultTimestampedIdentifiableRepositoryObserver;
import com.strategicgains.repoexpress.event.UuidIdentityRepositoryObserver;
import com.strategicgains.repoexpress.exception.DuplicateItemException;
import com.strategicgains.repoexpress.exception.InvalidObjectIdException;
import com.strategicgains.repoexpress.exception.ItemNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentRepository
        extends AbstractObservableRepository<Document>
{

    private Session session;

    private class Columns
    {

        static final String ID = "id";
        static final String OBJECT = "object";
        static final String CREATED_AT = "created_at";
        static final String UPDATED_AT = "updated_at";
    }

    private static final String EXISTENCE_CQL = "select count(*) from %s where %s = ?";
    private static final String READ_CQL = "select * from %s where %s = ?";
    private static final String READ_ALL_CQL = "select * from %s LIMIT %d";
    private static final String DELETE_CQL = "delete from %s where %s = ?";
    private static final String UPDATE_CQL = "update %s set object = ?, updated_at = ? where %s = ?";
    private static final String CREATE_CQL = "insert into %s (%s, object, created_at, updated_at) values (?, ?, ?, ?)";

    private final IndexBucketLocator bucketLocator;
    private static Logger logger = LoggerFactory.getLogger(DocumentRepository.class);

    public DocumentRepository(Session session)
    {
        super();
        this.session = session;
        this.bucketLocator = new SimpleIndexBucketLocatorImpl(200);//TODO: maybe we do actually want to let users set this
        addObserver(new UuidIdentityRepositoryObserver<Document>());
        addObserver(new DefaultTimestampedIdentifiableRepositoryObserver<Document>());
        addObserver(new StateChangeEventingObserver<Document>(new DocumentEventFactory()));
    }

    protected Session session()
    {
        return session;
    }

    @Override
    public Document doCreate(Document entity)
    {
        if (exists(entity.getId()))
        {
            throw new DuplicateItemException(entity.getClass().getSimpleName()
                    + " ID already exists: " + entity.getId().toString());
        }

        Table table = entity.table();
        PreparedStatement createStmt = PreparedStatementFactory.getPreparedStatement(String.format(CREATE_CQL, table.toDbTable(), Columns.ID), session());

        BoundStatement bs = new BoundStatement(createStmt);
        bindCreate(bs, entity);
        BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
        batch.add(bs);//the actual create
        List<BoundStatement> indexStatements = IndexMaintainerHelper.generateDocumentCreateIndexEntriesStatements(session, entity, bucketLocator);
        for (BoundStatement boundIndexStatement : indexStatements)
        {
            batch.add(boundIndexStatement);//the index creates
        }
        session().execute(batch);
        return entity;
    }

    @Override
    public Document doRead(Identifier identifier)
    {
        Table table = extractTable(identifier);
        Identifier id = extractId(identifier);
        PreparedStatement readStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_CQL, table.toDbTable(), Columns.ID), session());

        BoundStatement bs = new BoundStatement(readStmt);
        bindIdentifier(bs, id);
        Document item = marshalRow(session().execute(bs).one());

        if (item == null)
        {
            throw new ItemNotFoundException("ID not found: " + identifier.toString());
        }
        //item.setId(identifier);
        item.table(table);
        return item;
    }

    public QueryResponseWrapper doReadAll(String database, String tableString, int limit, long offset)
    {
        Table table = new Table();
        table.database(database);
        table.name(tableString);
        long maxIndex = offset + limit;
        PreparedStatement readStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_ALL_CQL, table.toDbTable(), maxIndex + 1), session());//we do one plus here so we know if there are additional results
        BoundStatement bs = new BoundStatement(readStmt);
        //run the query
        ResultSet results = session.execute(bs);
        
        return parseResultSetWithLimitAndOffset(results, limit, offset);
    }

    public static QueryResponseWrapper parseResultSetWithLimitAndOffset(ResultSet results, int limit, long offset)
    {
        //process result(s)
        long maxIndex = offset + limit;
        ArrayList<Document> toReturn = new ArrayList<>(limit);
        Iterator<Row> ite = results.iterator();
        long offsetCounter = 0;
        Long additionalResults = 0l;//default to 0, will be set to null if there are additional results (or in a later implementation, the actual number)
        while (ite.hasNext())//for each item in the result set
        {
            Row row = ite.next();
            if (offsetCounter >= maxIndex)//if we are at a counter less than our max amount to return (offset + limit)
            {
                additionalResults = null;//we have additional results for sure (see comment about +1 limit)
                break;//we are done; don't bother processing anymore, it's not going to be used anyway
            } else if (offsetCounter >= offset)//if we are at a counter greater than or equal to our offset -- we are in the sweet spot of the result set to return
            {
                toReturn.add(DocumentRepository.marshalRow(row));//we can add it to our return list
            } else
            {
                if (logger.isTraceEnabled())
                {
                    logger.trace("We are probably wasting processor time by processing a query inefficently");//TODO: obviously, consider improving this (or at least take out the logger if we decide not to)
                }
            }
            offsetCounter++;

        }
        return new QueryResponseWrapper(toReturn, additionalResults);
    }

    @Override
    public Document doUpdate(Document entity)
    {
        if (!exists(entity.getId()))
        {
            throw new ItemNotFoundException(entity.getClass().getSimpleName()
                    + " ID not found: " + entity.getId().toString());
        }

        Table table = entity.table();
        PreparedStatement updateStmt = PreparedStatementFactory.getPreparedStatement(String.format(UPDATE_CQL, table.toDbTable(), Columns.ID), session());

        BoundStatement bs = new BoundStatement(updateStmt);
        bindUpdate(bs, entity);
        BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
        batch.add(bs);//the actual create
        List<BoundStatement> indexStatements = IndexMaintainerHelper.generateDocumentUpdateIndexEntriesStatements(session, entity, bucketLocator);
        for (BoundStatement boundIndexStatement : indexStatements)
        {
            batch.add(boundIndexStatement);//the index updates
        }
        session().execute(batch);
        return entity;
    }

    @Override
    public void doDelete(Document entity)
    {
        try
        {
            Table table = entity.table();
            Identifier id = extractId(entity.getId());
            PreparedStatement deleteStmt = PreparedStatementFactory.getPreparedStatement(String.format(DELETE_CQL, table.toDbTable(), Columns.ID), session());

            BoundStatement bs = new BoundStatement(deleteStmt);
            bindIdentifier(bs, id);
            BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
            batch.add(bs);//the actual create
            List<BoundStatement> indexStatements = IndexMaintainerHelper.generateDocumentDeleteIndexEntriesStatements(session, entity, bucketLocator);
            for (BoundStatement boundIndexStatement : indexStatements)
            {
                batch.add(boundIndexStatement);//the index deletes
            }
            session().execute(batch);
        } catch (InvalidObjectIdException e)
        {
            throw new ItemNotFoundException("ID not found: " + entity.getId().toString());
        }
    }

    @Override
    public boolean exists(Identifier identifier)
    {
        if (identifier == null || identifier.isEmpty())
        {
            return false;
        }

        Table table = extractTable(identifier);
        Identifier id = extractId(identifier);
        PreparedStatement existStmt = PreparedStatementFactory.getPreparedStatement(String.format(EXISTENCE_CQL, table.toDbTable(), Columns.ID), session());

        BoundStatement bs = new BoundStatement(existStmt);
        bindIdentifier(bs, id);
        return (session().execute(bs).one().getLong(0) > 0);
    }

    private void bindIdentifier(BoundStatement bs, Identifier identifier)
    {
        bs.bind(identifier.primaryKey());
//		bs.bind(identifier.components().toArray());
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

    private Identifier extractId(Identifier identifier)
    {
		// This includes the date/version on the end...
//		List<Object> l = identifier.components().subList(2, 4);

        //TODO: determine what to do with version here.
        List<Object> l = identifier.components().subList(2, 3);
        return new Identifier(l.toArray());
    }

    private Table extractTable(Identifier identifier)
    {
        Table t = new Table();
        List<Object> l = identifier.components().subList(0, 2);//NOTE/TODO: frequent IndexOutOfBounds here
        t.database((String) l.get(0));
        t.name((String) l.get(1));
        return t;
    }

    public static Document marshalRow(Row row)
    {
        if (row == null)
        {
            return null;
        }

        Document d = new Document();
        d.setUuid(row.getUUID(Columns.ID));
        ByteBuffer b = row.getBytes(Columns.OBJECT);

        if (b != null && b.hasArray())
        {
            byte[] result = new byte[b.remaining()];
            b.get(result);
            BSONObject o = BSON.decode(result);
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

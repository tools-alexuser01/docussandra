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
import com.strategicgains.docussandra.bucketmanagement.IndexBucketLocator;
import com.strategicgains.docussandra.bucketmanagement.SimpleIndexBucketLocatorImpl;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.event.DocumentCreatedEvent;
import com.strategicgains.docussandra.event.DocumentDeletedEvent;
import com.strategicgains.docussandra.event.DocumentUpdatedEvent;
import com.strategicgains.docussandra.event.EventFactory;
import com.strategicgains.docussandra.handler.IndexMaintainerHelper;
import com.strategicgains.repoexpress.AbstractObservableRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.event.DefaultTimestampedIdentifiableRepositoryObserver;
import com.strategicgains.repoexpress.event.UuidIdentityRepositoryObserver;
import com.strategicgains.repoexpress.exception.DuplicateItemException;
import com.strategicgains.repoexpress.exception.InvalidObjectIdException;
import com.strategicgains.repoexpress.exception.ItemNotFoundException;

public class DocumentRepository
        extends AbstractObservableRepository<Document> {

    private Session session;

    private class Columns {

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

    private final IndexBucketLocator bucketLocator;

    public DocumentRepository(Session session) {
        super();
        this.session = session;
        this.bucketLocator = new SimpleIndexBucketLocatorImpl(200);//TODO: maybe we do actually want to let users set this
        addObserver(new UuidIdentityRepositoryObserver<Document>());
        addObserver(new DefaultTimestampedIdentifiableRepositoryObserver<Document>());
        addObserver(new StateChangeEventingObserver<Document>(new DocumentEventFactory()));
    }

    protected Session session() {
        return session;
    }

    @Override
    public Document doCreate(Document entity) {
        if (exists(entity.getId())) {
            throw new DuplicateItemException(entity.getClass().getSimpleName()
                    + " ID already exists: " + entity.getId().toString());
        }

        Table table = entity.table();
        PreparedStatement createStmt = createStmts.get(table);

        if (createStmt == null) {
            createStmt = session().prepare(String.format(CREATE_CQL, table.toDbTable(), Columns.ID));
            createStmts.put(table, createStmt);//TODO: udeyoje: This seems dangrous (memory overrun); should we use a caching lib instead?
        }

        BoundStatement bs = new BoundStatement(createStmt);
        bindCreate(bs, entity);
        session().execute(bs);
        List<BoundStatement> indexStatements = IndexMaintainerHelper.generateDocumentCreateIndexEntriesStatements(session, entity, bucketLocator);
        for (BoundStatement boundIndexStatement : indexStatements) {
            session().execute(boundIndexStatement);//TODO: run in batch -- issue #36
        }
        return entity;
    }

    @Override
    public Document doRead(Identifier identifier) {
        Table table = extractTable(identifier);
        Identifier id = extractId(identifier);
        PreparedStatement readStmt = readStmts.get(table);

        if (readStmt == null) {
            readStmt = session().prepare(String.format(READ_CQL, table.toDbTable(), Columns.ID));
            readStmts.put(table, readStmt);
        }

        BoundStatement bs = new BoundStatement(readStmt);
        bindIdentifier(bs, id);
        Document item = marshalRow(session().execute(bs).one());

        if (item == null) {
            throw new ItemNotFoundException("ID not found: " + identifier.toString());
        }

        return item;
    }

    @Override
    public Document doUpdate(Document entity) {
        if (!exists(entity.getId())) {
            throw new ItemNotFoundException(entity.getClass().getSimpleName()
                    + " ID not found: " + entity.getId().toString());
        }

        Table table = entity.table();
        PreparedStatement updateStmt = updateStmts.get(table);

        if (updateStmt == null) {
            updateStmt = session().prepare(String.format(UPDATE_CQL, table.toDbTable(), Columns.ID));
            updateStmts.put(table, updateStmt);
        }

        BoundStatement bs = new BoundStatement(updateStmt);
        bindUpdate(bs, entity);
        session().execute(bs);
        List<BoundStatement> indexStatements = IndexMaintainerHelper.generateDocumentUpdateIndexEntriesStatements(session, entity, bucketLocator);
        for (BoundStatement boundIndexStatement : indexStatements) {
            session().execute(boundIndexStatement);//TODO: run in batch -- issue #36
        }
        return entity;
    }

    @Override
    public void doDelete(Document entity) {
        try {
            Table table = entity.table();
            Identifier id = extractId(entity.getId());
            PreparedStatement deleteStmt = deleteStmts.get(table);

            if (deleteStmt == null) {
                deleteStmt = session().prepare(String.format(DELETE_CQL, table.toDbTable(), Columns.ID));
                deleteStmts.put(table, deleteStmt);
            }

            BoundStatement bs = new BoundStatement(deleteStmt);
            bindIdentifier(bs, id);
            session().execute(bs);
            List<BoundStatement> indexStatements = IndexMaintainerHelper.generateDocumentDeleteIndexEntriesStatements(session, entity, bucketLocator);
            for (BoundStatement boundIndexStatement : indexStatements) {
                session().execute(boundIndexStatement);//TODO: run in batch -- issue #36
            }
        } catch (InvalidObjectIdException e) {
            throw new ItemNotFoundException("ID not found: " + entity.getId().toString());
        }
    }

    @Override
    public boolean exists(Identifier identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return false;
        }

        Table table = extractTable(identifier);
        Identifier id = extractId(identifier);
        PreparedStatement existStmt = existsStmts.get(table);

        if (existStmt == null) {
            existStmt = session().prepare(String.format(EXISTENCE_CQL, table.toDbTable(), Columns.ID));
            existsStmts.put(table, existStmt);
        }

        BoundStatement bs = new BoundStatement(existStmt);
        bindIdentifier(bs, id);
        return (session().execute(bs).one().getLong(0) > 0);
    }

    private void bindIdentifier(BoundStatement bs, Identifier identifier) {
        bs.bind(identifier.primaryKey());
//		bs.bind(identifier.components().toArray());
    }

    private void bindCreate(BoundStatement bs, Document entity) {
        BSONObject bson = (BSONObject) JSON.parse(entity.object());
        bs.bind(entity.getUuid(),
                ByteBuffer.wrap(BSON.encode(bson)),
                entity.getCreatedAt(),
                entity.getUpdatedAt());
    }

    private void bindUpdate(BoundStatement bs, Document entity) {
        BSONObject bson = (BSONObject) JSON.parse(entity.object());

        bs.bind(ByteBuffer.wrap(BSON.encode(bson)),
                entity.getUpdatedAt(),
                entity.getUuid());
    }

    private Identifier extractId(Identifier identifier) {
		// This includes the date/version on the end...
//		List<Object> l = identifier.components().subList(2, 4);

        //TODO: determine what to do with version here.
        List<Object> l = identifier.components().subList(2, 3);
        return new Identifier(l.toArray());
    }

    private Table extractTable(Identifier identifier) {
        Table t = new Table();
        List<Object> l = identifier.components().subList(0, 2);
        t.database((String) l.get(0));
        t.name((String) l.get(1));
        return t;
    }

    private Document marshalRow(Row row) {
        if (row == null) {
            return null;
        }

        Document d = new Document();
        d.setUuid(row.getUUID(Columns.ID));
        ByteBuffer b = row.getBytes(Columns.OBJECT);

        if (b != null && b.hasArray()) {
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
            implements EventFactory<Document> {

        @Override
        public Object newCreatedEvent(Document object) {
            return new DocumentCreatedEvent(object);
        }

        @Override
        public Object newUpdatedEvent(Document object) {
            return new DocumentUpdatedEvent(object);
        }

        @Override
        public Object newDeletedEvent(Document object) {
            return new DocumentDeletedEvent(object);
        }
    }
}

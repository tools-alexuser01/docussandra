package com.strategicgains.docussandra.persistence;

import com.datastax.driver.core.BatchStatement;
import java.nio.ByteBuffer;
import java.util.List;

import org.bson.BSON;
import org.bson.BSONObject;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.mongodb.util.JSON;
import com.strategicgains.docussandra.bucketmanagement.IndexBucketLocator;
import com.strategicgains.docussandra.bucketmanagement.SimpleIndexBucketLocatorImpl;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.DocumentIdentifier;
import com.strategicgains.docussandra.domain.Identifier;
import com.strategicgains.docussandra.domain.QueryResponseWrapper;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.exception.DuplicateItemException;
import com.strategicgains.docussandra.exception.IndexParseException;
import com.strategicgains.docussandra.exception.InvalidObjectIdException;
import com.strategicgains.docussandra.exception.ItemNotFoundException;
import com.strategicgains.docussandra.handler.IndexMaintainerHelper;
import com.strategicgains.docussandra.persistence.helper.DocumentPersistanceUtils;
import com.strategicgains.docussandra.persistence.helper.PreparedStatementFactory;
import com.strategicgains.docussandra.persistence.parent.AbstractCRUDRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repository for interacting with documents.
 * @author udeyoje
 */
public class DocumentRepository extends AbstractCRUDRepository<Document>
{

    private Session session;

    public class Columns
    {

        public static final String ID = "id";
        public static final String OBJECT = "object";
        public static final String CREATED_AT = "created_at";
        public static final String UPDATED_AT = "updated_at";
    }

    private static final String EXISTENCE_CQL = "select count(*) from %s where %s = ?";
    private static final String READ_CQL = "select * from %s where %s = ? ORDER BY updated_at DESC";
    private static final String READ_ALL_CQL = "select * from %s LIMIT %d";

    private static final String DELETE_CQL = "delete from %s where %s = ?";
    //private static final String UPDATE_CQL = "update %s set object = ?, updated_at = ? where %s = ?";
    private static final String CREATE_CQL = "insert into %s (%s, object, created_at, updated_at) values (?, ?, ?, ?)";

    private final IndexBucketLocator bucketLocator;
    private static Logger logger = LoggerFactory.getLogger(DocumentRepository.class);

    /**
     * Constructor.
     * @param session 
     */
    public DocumentRepository(Session session)
    {
        super(session);
        this.session = session;
        this.bucketLocator = new SimpleIndexBucketLocatorImpl(200);//TODO: maybe we do actually want to let users set this
    }

    @Override
    public Document create(Document entity)
    {
        if (exists(entity.getId()))
        {
            throw new DuplicateItemException(entity.getClass().getSimpleName()
                    + " ID already exists: " + entity.getId().toString());
        }

        Table table = entity.table();
        PreparedStatement createStmt = PreparedStatementFactory.getPreparedStatement(String.format(CREATE_CQL, table.toDbTable(), Columns.ID), getSession());
        try
        {
            BoundStatement bs = new BoundStatement(createStmt);
            bindCreate(bs, entity);
            BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
            batch.add(bs);//the actual create
            List<BoundStatement> indexStatements = IndexMaintainerHelper.generateDocumentCreateIndexEntriesStatements(session, entity, bucketLocator);
            for (BoundStatement boundIndexStatement : indexStatements)
            {
                batch.add(boundIndexStatement);//the index creates
            }
            getSession().execute(batch);
            return entity;
        } catch (IndexParseException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public Document read(Identifier identifier)
    {
        Table table = identifier.getTable();
        PreparedStatement readStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_CQL, table.toDbTable(), Columns.ID), getSession());

        BoundStatement bs = new BoundStatement(readStmt);
        bindIdentifier(bs, identifier);
        Document item = DocumentPersistanceUtils.marshalRow(getSession().execute(bs).one());

        if (item == null)
        {
            throw new ItemNotFoundException("ID not found: " + identifier.toString());
        }
        //item.setId(identifier);
        item.table(table);
        return item;
    }

    public QueryResponseWrapper readAll(String database, String tableString, int limit, long offset)
    {
        Table table = new Table();
        table.database(database);
        table.name(tableString);
        long maxIndex = offset + limit;
        PreparedStatement readStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_ALL_CQL, table.toDbTable(), maxIndex + 1), getSession());//we do one plus here so we know if there are additional results
        BoundStatement bs = new BoundStatement(readStmt);
        //run the query
        ResultSet results = session.execute(bs);

        return DocumentPersistanceUtils.parseResultSetWithLimitAndOffset(results, limit, offset);
    }

    @Override
    public Document update(Document entity)
    {
        Document old = read(entity.getId()); //will throw exception of doc is not found
        entity.setCreatedAt(old.getCreatedAt());//copy over the original create date
        Table table = entity.table();
        PreparedStatement updateStmt = PreparedStatementFactory.getPreparedStatement(String.format(CREATE_CQL, table.toDbTable(), Columns.ID), getSession());

        BoundStatement bs = new BoundStatement(updateStmt);
        bindCreate(bs, entity);
        BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
        batch.add(bs);//the actual update
        try
        {
            List<BoundStatement> indexStatements = IndexMaintainerHelper.generateDocumentUpdateIndexEntriesStatements(session, entity, bucketLocator);
            for (BoundStatement boundIndexStatement : indexStatements)
            {
                batch.add(boundIndexStatement);//the index updates
            }
            getSession().execute(batch);
            return entity;
        } catch (IndexParseException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(Document entity)
    {
        try
        {
            Table table = entity.table();
            PreparedStatement deleteStmt = PreparedStatementFactory.getPreparedStatement(String.format(DELETE_CQL, table.toDbTable(), Columns.ID), getSession());

            BoundStatement bs = new BoundStatement(deleteStmt);
            bindIdentifier(bs, entity.getId());
            BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
            batch.add(bs);//the actual delete
            try
            {
                List<BoundStatement> indexStatements = IndexMaintainerHelper.generateDocumentDeleteIndexEntriesStatements(session, entity, bucketLocator);
                for (BoundStatement boundIndexStatement : indexStatements)
                {
                    batch.add(boundIndexStatement);//the index deletes
                }
                getSession().execute(batch);
            } catch (IndexParseException e)
            {
                throw new RuntimeException(e);//this shouldn't actually happen outside of tests
            }
        } catch (InvalidObjectIdException e)
        {
            throw new ItemNotFoundException("ID not found: " + entity.getId().toString());
        }
    }

    @Override
    public void delete(Identifier id)
    {
        //ok, this is kinda messed up; we actually need to FETCH the document in
        //order to delete it, otherwise we can't determine what iTables need to
        //be updated
        Document entity = this.read(id);
        try
        {
            Table table = entity.table();
            PreparedStatement deleteStmt = PreparedStatementFactory.getPreparedStatement(String.format(DELETE_CQL, table.toDbTable(), Columns.ID), getSession());

            BoundStatement bs = new BoundStatement(deleteStmt);
            bindIdentifier(bs, id);
            BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
            batch.add(bs);//the actual delete
            try
            {
                List<BoundStatement> indexStatements = IndexMaintainerHelper.generateDocumentDeleteIndexEntriesStatements(session, entity, bucketLocator);
                for (BoundStatement boundIndexStatement : indexStatements)
                {
                    batch.add(boundIndexStatement);//the index deletes
                }
                getSession().execute(batch);
            } catch (IndexParseException e)
            {
                throw new RuntimeException(e);//this shouldn't actually happen outside of tests
            }
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

        Table table = identifier.getTable();
        //Identifier id = extractId(identifier);
        PreparedStatement existStmt = PreparedStatementFactory.getPreparedStatement(String.format(EXISTENCE_CQL, table.toDbTable(), Columns.ID), getSession());

        BoundStatement bs = new BoundStatement(existStmt);
        bindIdentifier(bs, identifier);
        return (getSession().execute(bs).one().getLong(0) > 0);
    }

    @Override
    protected void bindIdentifier(BoundStatement bs, Identifier identifier)
    {
        DocumentIdentifier docId = new DocumentIdentifier(identifier);
        bs.bind(docId.getUUID());
    }

    private void bindCreate(BoundStatement bs, Document entity)
    {
        BSONObject bson = (BSONObject) JSON.parse(entity.object());
        bs.bind(entity.getUuid(),
                ByteBuffer.wrap(BSON.encode(bson)),
                entity.getCreatedAt(),
                entity.getUpdatedAt());
    }

}

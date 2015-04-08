package com.strategicgains.docussandra.handler;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.strategicgains.docussandra.Utils;
import com.strategicgains.docussandra.bucketmanagement.IndexBucketLocator;
import com.strategicgains.docussandra.cache.CacheFactory;
import com.strategicgains.docussandra.cache.CacheSynchronizer;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.persistence.DocumentRepository;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.helper.PreparedStatementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.bson.BSON;
import org.bson.BSONObject;
import org.restexpress.common.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EventHandler for maintaining indices (really just additional tables with the
 * same data) after CRUD events on documents.
 *
 * @author udeyoje
 */
public class IndexMaintainerHelper
{

    private static Logger logger = LoggerFactory.getLogger(IndexMaintainerHelper.class);

    public static final String ITABLE_INSERT_CQL = "INSERT INTO docussandra.%s (bucket, id, object, created_at, updated_at, %s) VALUES (?, ?, ?, ?, ?, %s);";
    //TODO: --------------------remove hard coding of keyspace name--^^^----
    public static final String ITABLE_UPDATE_CQL = "UPDATE docussandra.%s SET object = ?, updated_at = ? WHERE bucket = ? AND %s;";
    //TODO: ----------------remove hard coding of keyspace name--^^^--------
    public static final String ITABLE_DELETE_CQL = "DELETE FROM docussandra.%s WHERE bucket = ? AND %s;";
    //TODO: ----------------remove hard coding of keyspace name--^^^--------

    public static List<BoundStatement> generateDocumentCreateIndexEntriesStatements(Session session, Document entity, IndexBucketLocator bucketLocator)
    {
        //check for any indices that should exist on this table per the index table
        List<Index> indices = getIndexForDocument(session, entity);
        ArrayList<BoundStatement> statementList = new ArrayList<>(indices.size());
        //for each index
        for (Index index : indices)
        {
            //add row to the iTable(s)
            BoundStatement bs = generateDocumentCreateIndexEntryStatement(session, index, entity, bucketLocator);
            if (bs != null)
            {
                statementList.add(bs);
            }
        }
        //return a list of commands to accomplish all of this
        return statementList;
    }

    /**
     * Helper method for above.
     *
     * @param session
     * @param index
     * @param entity
     * @return
     */
    private static BoundStatement generateDocumentCreateIndexEntryStatement(Session session, Index index, Document entity, IndexBucketLocator bucketLocator)
    {
        //determine which fields need to write as PKs
        List<String> fields = index.fields();
        String finalCQL = getCQLStatementForInsert(index);
        PreparedStatement ps = PreparedStatementFactory.getPreparedStatement(finalCQL, session);
        BoundStatement bs = new BoundStatement(ps);
        //pull the index fields out of the document for binding
        String documentJSON = entity.object();
        DBObject jsonObject = (DBObject) JSON.parse(documentJSON);
        //set the bucket
        Object fieldToBucketOnObject = jsonObject.get(fields.get(0));
        if (fieldToBucketOnObject == null)
        {
            // we do not have an indexable field in our document -- therefore, it shouldn't be added to an index! (right?) -- is this right Todd?
            logger.trace("Warning: document: " + entity.toString() + " does not have an indexed field for index: " + index.toString());
            return null;
        }
        String fieldToBucketOn = fieldToBucketOnObject.toString();//use the java toString to convert the object to a string.
        String bucketId = bucketLocator.getBucket(null, Utils.convertStringToFuzzyUUID(fieldToBucketOn));//note, could have parse problems here with non-string types
        if (logger.isTraceEnabled())
        {
            logger.trace("Bucket ID for entity: " + entity.toString() + "for index: " + index.toString() + " is: " + bucketId);
        }
        bs.setString(0, bucketId);
        //set the id
        bs.setUUID(1, entity.getUuid());
        //set the blob
        BSONObject bson = (BSONObject) JSON.parse(entity.object());
        bs.setBytes(2, ByteBuffer.wrap(BSON.encode(bson)));
        //set the dates
        bs.setDate(3, entity.getCreatedAt());
        bs.setDate(4, entity.getUpdatedAt());
        for (int i = 0; i < fields.size(); i++)
        {
            String field = fields.get(i);
            Object jObject = jsonObject.get(field);

            if (jObject == null)
            {
                bs.setString(i + 5, "");//offset from the first five non-dynamic fields
            } else
            {
                String fieldValue = jObject.toString();//note, could have parse problems here with non-string types: TODO: use proper types; need to set the tables correctly first
                bs.setString(i + 5, fieldValue);//offset from the first five non-dynamic fields
            }

        }
        return bs;
    }

    public static List<BoundStatement> generateDocumentUpdateIndexEntriesStatements(Session session, Document entity, IndexBucketLocator bucketLocator)
    {
        //check for any indices that should exist on this table per the index table
        List<Index> indices = getIndexForDocument(session, entity);
        ArrayList<BoundStatement> statementList = new ArrayList<>(indices.size());
        //for each index
        for (Index index : indices)
        {
            //determine which fields need to use as PKs
            List<String> fields = index.fields();

            //issue #35: we need to be able to update indexed fields as well,
            //which will require us to:
            //1. determine if an indexed field has changed
            if (hasIndexedFieldChanged(session, index, entity))
            {
                //2a. if the field has changed, create a new index entry
                BoundStatement bs = generateDocumentCreateIndexEntryStatement(session, index, entity, bucketLocator);
                if (bs != null)
                {
                    statementList.add(bs);
                }
                //2b. after creating the new index entry, we must delete the old one
                statementList.add(generateDocumentDeleteIndexEntryStatement(session, index, entity, bucketLocator));
            } else
            {//3. if an indexed field has not changed, do a normal CQL update
                String finalCQL = getCQLStatementForWhereClauses(ITABLE_UPDATE_CQL, index);
                PreparedStatement ps = PreparedStatementFactory.getPreparedStatement(finalCQL, session);
                BoundStatement bs = new BoundStatement(ps);

                //set the blob
                BSONObject bson = (BSONObject) JSON.parse(entity.object());
                bs.setBytes(0, ByteBuffer.wrap(BSON.encode(bson)));
                //set the date
                bs.setDate(1, entity.getUpdatedAt());
                //pull the index fields out of the document for binding
                String documentJSON = entity.object();
                DBObject jsonObject = (DBObject) JSON.parse(documentJSON);
                //set the bucket
                String bucketId = bucketLocator.getBucket(null, Utils.convertStringToFuzzyUUID((String) jsonObject.get(fields.get(0))));//note, could have parse problems here with non-string types
                logger.debug("Bucket ID for entity: " + entity.toString() + " for index: " + index.toString() + " is: " + bucketId);
                bs.setString(2, bucketId);
                for (int i = 0; i < fields.size(); i++)
                {
                    String field = fields.get(i);
                    String fieldValue = (String) jsonObject.get(field);//note, could have parse problems here with non-string types
                    bs.setString(i + 3, fieldValue);//offset from the first three non-dynamic fields
                }
                //add row to the iTable(s)
                statementList.add(bs);
            }
        }
        //return a list of commands to accomplish all of this
        return statementList;
    }

    public static List<BoundStatement> generateDocumentDeleteIndexEntriesStatements(Session session, Document entity, IndexBucketLocator bucketLocator)
    {
        //check for any indices that should exist on this table per the index table
        List<Index> indices = getIndexForDocument(session, entity);
        ArrayList<BoundStatement> statementList = new ArrayList<>(indices.size());
        //for each index
        for (Index index : indices)
        {
            BoundStatement bs = generateDocumentDeleteIndexEntryStatement(session, index, entity, bucketLocator);
            if (bs != null)
            {
                statementList.add(bs);
            }
        }
        return statementList;
    }

    /**
     * Helper method for above.
     *
     * @param session
     * @param entity
     * @return
     */
    private static BoundStatement generateDocumentDeleteIndexEntryStatement(Session session, Index index, Document entity, IndexBucketLocator bucketLocator)
    {
        //determine which fields need to write as PKs
        List<String> fields = index.fields();
        String finalCQL = getCQLStatementForWhereClauses(ITABLE_DELETE_CQL, index);
        PreparedStatement ps = PreparedStatementFactory.getPreparedStatement(finalCQL, session);
        BoundStatement bs = new BoundStatement(ps);
        //pull the index fields out of the document for binding
        String documentJSON = entity.object();
        DBObject jsonObject = (DBObject) JSON.parse(documentJSON);
        Object fieldToBucketOnObject = jsonObject.get(fields.get(0));
        if (fieldToBucketOnObject == null)
        {
            // we do not have an indexable field in our document -- therefore, it shouldn't need to be removed an index! (right?) -- is this right Todd?
            logger.trace("Warning: document: " + entity.toString() + " does not have an indexed field for index: " + index.toString());
            return null;
        }
        //set the bucket
        String bucketId = bucketLocator.getBucket(null, Utils.convertStringToFuzzyUUID(fieldToBucketOnObject.toString()));//note, could have parse problems here with non-string types
        logger.debug("Bucket ID for entity: " + entity.toString() + " for index: " + index.toString() + " is: " + bucketId);
        bs.setString(0, bucketId);
        for (int i = 0; i < fields.size(); i++)
        {
            String field = fields.get(i);
            String fieldValue = (String) jsonObject.get(field);//note, could have parse problems here with non-string types
            bs.setString(i + 1, fieldValue);
        }
        return bs;
    }

    //just a concept right now -- issue #4
    public static void populateNewIndexWithExistingData(Session session, Table t, Index index)
    {
        throw new UnsupportedOperationException("Not done yet");
    }

    /**
     * Gets all the indexes that a document is or needs to be stored in. Note
     * that this actually makes a database call.
     *
     * @param session Cassandra session for interacting with the database.
     * @param entity Document that we are trying to determine which indices it
     * is or should be stored in.
     * @return A list of Index objects where the document is or should be stored
     * in.
     */
    public static List<Index> getIndexForDocument(Session session, Document entity)
    {
        IndexRepository indexRepo = new IndexRepository(session);
        return indexRepo.readAllCached(entity.databaseName(), entity.tableName());
    }

    /**
     * Determines if an indexed field has changed as part of an update. This
     * would be private but keeping public for ease of testing.
     *
     * @param session DB session.
     * @param index Index containing the fields to check for changes.
     * @param entity New version of a document.
     * @return True if an indexed field has changed. False if there is no change
     * of indexed fields.
     */
    public static boolean hasIndexedFieldChanged(Session session, Index index, Document entity)
    {
        DocumentRepository docRepo = new DocumentRepository(session);//TODO: if we do any sycronization on doc repo, this could be a problem
        BSONObject newObject = (BSONObject) JSON.parse(entity.object());
        BSONObject oldObject = (BSONObject) JSON.parse(docRepo.doRead(entity.getId()).object());
        for (String field : index.fields())
        {
            if (!newObject.get(field).equals(oldObject.get(field)))
            {
                return true;//fail early
            }
        }
        return false;
    }

    /**
     * Helper for generating insert CQL statements for iTables. This would be
     * private but keeping public for ease of testing. Same as
     * generateCQLStatementForInsert but will retrieve from cache if available.
     *
     * @param index Index to generate the statement for.
     * @return CQL statement.
     */
    public static String getCQLStatementForInsert(Index index)
    {
        String key = index.databaseName() + ":" + index.tableName() + ":" + index.name();
        Cache iTableCQLCache = CacheFactory.getCache("iTableInsertCQL");
        //synchronized (CacheSynchronizer.getLockingObject(key, "iTableInsertCQL"))
        //{
        Element e = iTableCQLCache.get(key);
        if (e == null || e.getObjectValue() == null)//if its not set, or set, but null, re-read
        {
            //not cached; let's create it                               
            e = new Element(key, generateCQLStatementForInsert(index));//save it back to the cache
            iTableCQLCache.put(e);
        } else
        {
            logger.trace("Pulling iTableInsertCQL from Cache: " + e.getObjectValue().toString());
        }
        return (String) e.getObjectValue();
        //}
    }

    /**
     * Helper for generating insert CQL statements for iTables. This would be
     * private but keeping public for ease of testing.
     *
     * @param index Index to generate the statement for.
     * @return CQL statement.
     */
    public static String generateCQLStatementForInsert(Index index)
    {
        //determine which iTables need to be written to
        String iTableToUpdate = Utils.calculateITableName(index);
        //determine which fields need to write as PKs
        List<String> fields = index.fields();
        String fieldNamesInsertSyntax = StringUtils.join(",", fields);
        //calculate the number of '?'s we need to append on the values clause
        StringBuilder fieldValueInsertSyntax = new StringBuilder();
        for (int i = 0; i < fields.size(); i++)
        {
            if (i != 0)
            {
                fieldValueInsertSyntax.append(", ");
            }
            fieldValueInsertSyntax.append("?");
        }
        //create final CQL statement for adding a row to an iTable(s)
        return String.format(ITABLE_INSERT_CQL, iTableToUpdate, fieldNamesInsertSyntax, fieldValueInsertSyntax);
    }

    /**
     * Helper for generating update CQL statements for iTables. This would be
     * private but keeping public for ease of testing. Same as
     * generateCQLStatementForWhereClauses but will retrieve from cache when
     * availible.
     *
     * @param CQL statement that is not yet formatted.
     * @param index Index to generate the statement for.
     * @return CQL statement.
     */
    public static String getCQLStatementForWhereClauses(String CQL, Index index)
    {
        String key = index.databaseName() + ":" + index.tableName() + ":" + index.name();
        String whereClause;
        String iTableToUpdate = Utils.calculateITableName(index);
        Cache whereCache = CacheFactory.getCache("iTableWhere");
//        synchronized (CacheSynchronizer.getLockingObject(key, "iTableWhere"))
//        {
        Element e = whereCache.get(key);
        if (e == null || e.getObjectValue() == null)//if its not set, or set, but null, re-read
        {
            //not cached; let's create it                               
            e = new Element(key, getWhereClauseHelper(index));//save it back to the cache
            whereCache.put(e);
        } else
        {
            logger.trace("Pulling WHERE statement info from Cache: " + e.getObjectValue().toString());
        }
        whereClause = (String) e.getObjectValue();
//        }
        //create final CQL statement for updating a row in an iTable(s)        
        return String.format(CQL, iTableToUpdate, whereClause);
    }

    /**
     * Helper for generating update CQL statements for iTables. This would be
     * private but keeping public for ease of testing.
     *
     * @param CQL statement that is not yet formatted.
     * @param index Index to generate the statement for.
     * @return CQL statement.
     */
    public static String generateCQLStatementForWhereClauses(String CQL, Index index)
    {
        //determine which iTables need to be updated
        String iTableToUpdate = Utils.calculateITableName(index);
        //create final CQL statement for updating a row in an iTable(s)        
        return String.format(CQL, iTableToUpdate, getWhereClauseHelper(index));
    }

    private static String getWhereClauseHelper(Index index)
    {
        //determine which fields need to write as PKs
        List<String> fields = index.fields();
        //determine the where clause
        StringBuilder setValues = new StringBuilder();
        for (int i = 0; i < fields.size(); i++)
        {
            String field = fields.get(i);
            if (i != 0)
            {
                setValues.append(" AND ");
            }
            setValues.append(field).append(" = ?");
        }
        return setValues.toString();
    }

}

package com.strategicgains.docussandra.handler;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.strategicgains.docussandra.Utils;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.persistence.IndexRepository;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.bson.BSON;
import org.bson.BSONObject;
import org.restexpress.common.util.StringUtils;

/**
 * EventHandler for maintaining indices (really just additional tables with the
 * same data) after CRUD events on documents.
 *
 * @author udeyoje
 */
public class IndexMaintainerHelper { 

    public static final String ITABLE_INSERT_CQL = "INSERT INTO docussandra.%s (id, object, created_at, updated_at, %s) VALUES (?, ?, ?, ?, %s);";
    //TODO: --------------------remove hard coding of keyspace name--^^^----
    public static final String ITABLE_UPDATE_CQL = "UPDATE docussandra.%s SET object = ?, updated_at = ? WHERE %s;";
    //TODO: ----------------remove hard coding of keyspace name--^^^--------
    public static final String ITABLE_DELETE_CQL = "DELETE FROM docussandra.%s WHERE %s;";
    //TODO: ----------------remove hard coding of keyspace name--^^^--------

    public static List<BoundStatement> generateDocumentCreateIndexEntriesStatements(Session session, Document entity) {
        //check for any indices that should exist on this table per the index table
        List<Index> indices = getIndexForDocument(session, entity);
        ArrayList<BoundStatement> statementList = new ArrayList<>(indices.size());
        //for each index
        for (Index index : indices) {
            //determine which fields need to write as PKs
            List<String> fields = index.fields();
            String finalCQL = generateCQLStatementForInsert(index);
            PreparedStatement ps = session.prepare(finalCQL);
            BoundStatement bs = new BoundStatement(ps);

            //set the id
            bs.setUUID(0, entity.getUuid());
            //set the blob
            BSONObject bson = (BSONObject) JSON.parse(entity.object());
            bs.setBytes(1, ByteBuffer.wrap(BSON.encode(bson)));
            //set the dates
            bs.setDate(2, entity.getCreatedAt());
            bs.setDate(3, entity.getUpdatedAt());

            //pull the index fields out of the document for binding
            String documentJSON = entity.object();
            DBObject jsonObject = (DBObject) JSON.parse(documentJSON);
            for (int i = 0; i < fields.size(); i++) {
                String field = fields.get(i);
                String fieldValue = (String) jsonObject.get(field);//note, could have parse problems here with non-string types
                bs.setString(i + 4, fieldValue);//offset from the first four non-dynamic fields
            }
            //add row to the iTable(s)
            statementList.add(bs);
        }
        //return a list of commands to accomplish all of this
        return statementList;
    }

    public static List<BoundStatement> generateDocumentUpdateIndexEntriesStatements(Session session, Document entity) {
        
        //NOTE: This does not yet handle updating iTable entries where the indexed field has changed
        
        //check for any indices that should exist on this table per the index table
        List<Index> indices = getIndexForDocument(session, entity);
        ArrayList<BoundStatement> statementList = new ArrayList<>(indices.size());
        //for each index
        for (Index index : indices) {
            //determine which fields need to use as PKs
            List<String> fields = index.fields();
            String finalCQL = generateCQLStatementForWhereClauses(ITABLE_UPDATE_CQL, index);
            PreparedStatement ps = session.prepare(finalCQL);
            BoundStatement bs = new BoundStatement(ps);

            //set the blob
            BSONObject bson = (BSONObject) JSON.parse(entity.object());
            bs.setBytes(0, ByteBuffer.wrap(BSON.encode(bson)));
            //set the date
            bs.setDate(1, entity.getUpdatedAt());

            if (!index.isUnique()) { //if the index is not unique, use a where clause based on UUID
                bs.setUUID(2, entity.getUuid());
            } else { //if it is unique, we will use a where clause based on index
                //pull the index fields out of the document for binding
                String documentJSON = entity.object();
                DBObject jsonObject = (DBObject) JSON.parse(documentJSON);
                for (int i = 0; i < fields.size(); i++) {
                    String field = fields.get(i);
                    String fieldValue = (String) jsonObject.get(field);//note, could have parse problems here with non-string types
                    bs.setString(i + 2, fieldValue);//offset from the first two non-dynamic fields
                }
            }
            //add row to the iTable(s)
            statementList.add(bs);
        }
        //return a list of commands to accomplish all of this
        return statementList;
    }

    public static List<BoundStatement> generateDocumentDeleteIndexEntriesStatements(Session session, Document entity) {
        //check for any indices that should exist on this table per the index table
        List<Index> indices = getIndexForDocument(session, entity);
        ArrayList<BoundStatement> statementList = new ArrayList<>(indices.size());
        //for each index
        for (Index index : indices) {
            //determine which fields need to write as PKs
            List<String> fields = index.fields();
            String finalCQL = generateCQLStatementForWhereClauses(ITABLE_DELETE_CQL, index);
            PreparedStatement ps = session.prepare(finalCQL);
            BoundStatement bs = new BoundStatement(ps);
            if (!index.isUnique()) { //if the index is not unique, use a where clause based on UUID
                bs.setUUID(0, entity.getUuid());
            } else { //if it is unique, we will use a where clause based on index
                //pull the index fields out of the document for binding
                String documentJSON = entity.object();
                DBObject jsonObject = (DBObject) JSON.parse(documentJSON);
                for (int i = 0; i < fields.size(); i++) {
                    String field = fields.get(i);
                    String fieldValue = (String) jsonObject.get(field);//note, could have parse problems here with non-string types
                    bs.setString(i, fieldValue);
                }
            }
            statementList.add(bs);
        }
        return statementList;
    }

    //just a concept right now -- issue #4
    public static void populateNewIndexWithExistingData(Session session, Table t, Index index) {
        throw new UnsupportedOperationException("Not done yet");
    }

    /**
     * Gets all the indexes that a document is or needs to be stored in.
     *
     * @param session Cassandra session for interacting with the database.
     * @param entity Document that we are trying to determine which indices it
     * is or should be stored in.
     * @return A list of Index objects where the document is or should be stored
     * in.
     */
    public static List<Index> getIndexForDocument(Session session, Document entity) {
        IndexRepository indexRepo = new IndexRepository(session);
        return indexRepo.readAll(entity.databaseName(), entity.tableName());
    }

    /**
     * Helper for generating insert CQL statements for iTables. This would be
     * private but keeping public for ease of testing.
     *
     * @param index Index to generate the statement for.
     * @return CQL statement.
     */
    public static String generateCQLStatementForInsert(Index index) {
        //determine which iTables need to be written to
        String iTableToUpdate = Utils.calculateITableName(index);
        //determine which fields need to write as PKs
        List<String> fields = index.fields();
        String fieldNamesInsertSyntax = StringUtils.join(",", fields);
        //calculate the number of '?'s we need to append on the values clause
        StringBuilder fieldValueInsertSyntax = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            if (i != 0) {
                fieldValueInsertSyntax.append(", ");
            }
            fieldValueInsertSyntax.append("?");
        }
        //create final CQL statement for adding a row to an iTable(s)
        return String.format(ITABLE_INSERT_CQL, iTableToUpdate, fieldNamesInsertSyntax, fieldValueInsertSyntax);
    }

    /**
     * Helper for generating update CQL statements for iTables. This would be
     * private but keeping public for ease of testing.
     *
     * @param CQL statement that is not yet formatted.
     * @param index Index to generate the statement for.
     * @return CQL statement.
     */
    public static String generateCQLStatementForWhereClauses(String CQL, Index index) {
        //determine which iTables need to be updated
        String iTableToUpdate = Utils.calculateITableName(index);
        //determine which fields need to write as PKs
        List<String> fields = index.fields();
        //determine the where clause
        String whereClause;
        if (!index.isUnique()) {//if the index is not unique,
            whereClause = "id = ?";//we will use a where statement based on id
        } else {// if the index is unique, we use the field list
            StringBuilder setValues = new StringBuilder();
            for (int i = 0; i < fields.size(); i++) {
                String field = fields.get(i);
                if (i != 0) {
                    setValues.append(" AND ");
                }
                setValues.append(field).append(" = ?");
            }
            whereClause = setValues.toString();
        }
        //create final CQL statement for updating a row in an iTable(s)        
        return String.format(CQL, iTableToUpdate, whereClause);
    }

}

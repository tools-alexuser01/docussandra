package com.strategicgains.docussandra.handler;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.pearson.grid.pearsonlibrary.string.StringUtil;
import com.strategicgains.docussandra.Utils;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.bson.BSON;
import org.bson.BSONObject;

/**
 * EventHandler for maintaining indices (really just additional tables with the
 * same data) after CRUD events on documents.
 *
 * @author udeyoje
 */
public class IndexMaintainerHandler { //extends AbstractObservableRepository<Document> { //implements EventHandler {
    //note: we might not actually want to implement EventHandler
    //here, and we might want to move this to a helper package
    //(and change class name) or something

    private static final String ITABLE_INSERT_CQL = "INSERT INTO docussandra.%s (object, created_at, updated_at, %s) VALUES (?, ?, ?, %s);";    
    //TODO: --------------------remove hard coding of keyspace name--^^^----
    private static final String ITABLE_UPDATE_CQL = "UPDATE docussandra.%s SET object = ?, updated_at = ?, %s) WHERE %s;";
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

            //set the blob
            BSONObject bson = (BSONObject) JSON.parse(entity.object());
            bs.setBytes(0, ByteBuffer.wrap(BSON.encode(bson)));
            //set the dates
            bs.setDate(1, entity.getCreatedAt());
            bs.setDate(2, entity.getUpdatedAt());

            //pull the index fields out of the document for binding
            String documentJSON = entity.object();
            DBObject jsonObject = (DBObject) JSON.parse(documentJSON);
            for (int i = 0; i < fields.size(); i++) {
                String field = fields.get(i);
                String fieldValue = (String) jsonObject.get(field);//note, could have parse problems here with non-string types
                bs.setString(i + 3, fieldValue);//offset from the first three non-dynamic fields
            }
            //add row to the iTable(s)
            statementList.add(bs);
        }
        //return a list of commands to accomplish all of this
        return statementList;
    }

    public static List<BoundStatement> generateDocumentUpdateIndexEntriesStatements(Session session, Document entity) {
        Identifier id = entity.getId();
        throw new UnsupportedOperationException("Not done yet");
    }

    public static List<BoundStatement> generateDocumentDeleteIndexEntriesStatements(Session session, Document entity) {
        Identifier id = entity.getId();
        throw new UnsupportedOperationException("Not done yet");
    }

    //just a concept right now
    public static void reindex(Session session, Table t, Index index) {
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
     * Helper for generating insert CQL statements. This would be private but
     * keeping public for ease of testing.
     *
     * @param index Index to generate the statement for.
     * @return CQL statement.
     */
    public static String generateCQLStatementForInsert(Index index) {
        //determine which iTables need to be written to
        String iTableToUpdate = Utils.calculateITableName(index);
        //determine which fields need to write as PKs
        List<String> fields = index.fields();
        String fieldNamesInsertSyntax = StringUtil.combineList(fields);
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
}

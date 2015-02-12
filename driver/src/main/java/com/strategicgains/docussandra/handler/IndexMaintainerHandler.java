package com.strategicgains.docussandra.handler;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.repoexpress.domain.Identifier;
import java.util.List;

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
    
    public static List<BoundStatement> generateDocumentCreateIndexEntriesStatements(Session session, Document entity){
        Identifier id =  entity.getId();
        //check for any indices that should exist on this table per the index table
        
        //determine which index tables need to be written to       
        
        //add row to the index table(s)
        
        //return a list of commands to accomplish all of this
        throw new UnsupportedOperationException("Not done yet");
    }
    
    public static List<BoundStatement> generateDocumentUpdateIndexEntriesStatements(Session session, Document entity){
        Identifier id =  entity.getId();
        throw new UnsupportedOperationException("Not done yet");
    }
    
    public static List<BoundStatement> generateDocumentDeleteIndexEntriesStatements(Session session, Document entity){
        Identifier id =  entity.getId();
        throw new UnsupportedOperationException("Not done yet");
    }
    
    

}

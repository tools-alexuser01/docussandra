package com.strategicgains.docussandra.domain;

import java.util.ArrayList;

/**
 * Object that represents a CQL where clause to the extent that we need to process it.
 * @author udeyoje
 */
public class WhereClause {
    
    private final String whereClause;
    
    /**
     * Field Names in a where clause. Corresponds to values. 
     */
    private ArrayList<String> fields = new ArrayList<>();
    /**
     * Field Values in a where clause. Corresponds to fields. 
     */
    private ArrayList<String> values = new ArrayList<>();

    public WhereClause(String whereClause) {
        this.whereClause = whereClause;
        parse(whereClause);
    }
    
    private void parse(String whereClause){
    
    }
    
    
    
    public String getBoundStatementSyntax(){
        throw new UnsupportedOperationException("Not done yet");
    }

    /**
     * Field Names in a where clause. Corresponds to values.
     * @return the fields
     */
    public ArrayList<String> getFields() {
        return fields;
    }

    /**
     * Field Values in a where clause. Corresponds to fields.
     * @return the values
     */
    public ArrayList<String> getValues() {
        return values;
    }
    
    
    
}

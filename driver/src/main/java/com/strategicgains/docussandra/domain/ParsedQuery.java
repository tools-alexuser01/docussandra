/*
 * Copyright 2015 udeyoje.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.strategicgains.docussandra.domain;

import java.util.Objects;

/**
 * Object that represents a query that has been parsed for processing.
 * @author udeyoje
 */
public class ParsedQuery {
    
    /**
     * Original query that was passed in.
     */
    private Query query;
    
    /**
     * Parsed where clause for this query.
     */
    private WhereClause whereClause;
    
    /**
     * The iTable (index table) that needs to be queried in order to retrieve results.
     */
    private String iTable;   

    /**
     * Constructor.
     * @param query Original query that was passed in.
     * @param whereClause Parsed where clause for this query.
     * @param iTable The iTable (index table) that needs to be queried in order to retrieve results.
     */
    public ParsedQuery(Query query, WhereClause whereClause, String iTable) {
        this.query = query;
        this.whereClause = whereClause;
        this.iTable = iTable;
    }

    /**
     * Original query that was passed in.
     * @return 
     */
    public Query getQuery() {
        return query;
    }

    /**
     * Parsed where clause for this query.
     * @return 
     */
    public WhereClause getWhereClause() {
        return whereClause;
    }

    /**
     * The iTable (index table) that needs to be queried in order to retrieve results.
     * @return 
     */
    public String getITable() {
        return iTable;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 17 * hash + Objects.hashCode(this.query);
        hash = 17 * hash + Objects.hashCode(this.whereClause);
        hash = 17 * hash + Objects.hashCode(this.iTable);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ParsedQuery other = (ParsedQuery) obj;
        if (!Objects.equals(this.query, other.query)) {
            return false;
        }
        if (!Objects.equals(this.whereClause, other.whereClause)) {
            return false;
        }
        if (!Objects.equals(this.iTable, other.iTable)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "ParsedQuery{" + "query=" + query + ", whereClause=" + whereClause + ", iTable=" + iTable + '}';
    }
    
}

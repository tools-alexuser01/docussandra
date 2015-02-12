package com.strategicgains.docussandra;

import com.strategicgains.docussandra.domain.Index;

/**
 * A collection of public static helper methods for various docussandra related
 * tasks.
 *
 * @author udeyoje
 * @since Feb 12, 2015
 */
public class Utils {

    /**
     * Calculates the name of an iTable based on the dataBaseName, the tableName, and the indexName.
     *
     * @param databaseName dbName for the iTable.
     * @param tableName table name for the iTable. 
     * @param indexName index name for the iTable.
     * 
     * @return The name of the iTable for that index.
     */
    public static String calculateITableName(String databaseName, String tableName, String indexName) {
        StringBuilder sb = new StringBuilder();
        sb.append(databaseName);
        sb.append('_');
        sb.append(tableName);
        sb.append('_');
        sb.append(indexName);
        return sb.toString();
    }

    /**
     * Calculates the name of an iTable based on an index.
     *
     * @param index Index whose iTable name you would like.
     * @return The name of the iTable for that index.
     */
    public static String calculateITableName(Index index) {
        return calculateITableName(index.databaseName(), index.tableName(), index.name());
    }

}

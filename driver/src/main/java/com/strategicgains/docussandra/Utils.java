package com.strategicgains.docussandra;

import com.strategicgains.docussandra.cache.CacheFactory;
import com.strategicgains.docussandra.cache.CacheSynchronizer;
import com.strategicgains.docussandra.domain.Index;
import java.nio.ByteBuffer;
import java.util.UUID;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

/**
 * A collection of public static helper methods for various docussandra related
 * tasks.
 *
 * @author udeyoje
 * @since Feb 12, 2015
 */
public class Utils
{

    /**
     * Calculates the name of an iTable based on the dataBaseName, the
     * tableName, and the indexName.
     *
     * Note: No null checks.
     *
     * @param databaseName dbName for the iTable.
     * @param tableName table name for the iTable.
     * @param indexName index name for the iTable.
     *
     * @return The name of the iTable for that index.
     */
    public static String calculateITableName(String databaseName, String tableName, String indexName)
    {
        //String key = databaseName + ":" + tableName + ":" + indexName;
        //Cache c = CacheFactory.getCache("iTableName");
//        synchronized (CacheSynchronizer.getLockingObject(key, "iTableName"))
//        {
//            Element e = c.get(key);
//            if (e == null || e.getObjectValue() == null)//if its not set, or set, but null, re-read
//            {
                //not cached; let's create it
                StringBuilder sb = new StringBuilder();
                sb.append(databaseName);
                sb.append('_');
                sb.append(tableName);
                sb.append('_');
                sb.append(indexName);
                return sb.toString().toLowerCase();
//                e = new Element(key, sb.toString().toLowerCase());
//                c.put(e);
//            }
//            return (String) e.getObjectValue();
//        }
    }

    /**
     * Calculates the name of an iTable based on an index.
     *
     * Note: No null checks.
     *
     * @param index Index whose iTable name you would like.
     * @return The name of the iTable for that index.
     */
    public static String calculateITableName(Index index)
    {
        return calculateITableName(index.databaseName(), index.tableName(), index.name());
    }

//    public static List<String> parseWhereClause(String whereClause){ 
    //will have to parse more than the fields, but the field values as well!
//    
//    }
    /**
     * Converts a string to a fuzzy UUID. Fuzzy, as in it isn't going to be
     * unique and is only for the first 8 bytes. Should only be used for
     * bucketing. (We can add support for a second 8 bytes later if needed.)
     *
     * @param s String to convert.
     * @return a Fuzzy UUID to use for bucket placement.
     */
    public static UUID convertStringToFuzzyUUID(String s)
    {
        if (s == null)
        {
            return null;
        }
        s = s.toLowerCase();
        byte[] string = s.getBytes();
        if (string.length < 8)
        {//need to pad!
            byte[] newString = new byte[8];
            for (int i = 0; i < newString.length; i++)
            {
                if (i < string.length)
                {
                    newString[i] = string[i];
                } else
                {
                    newString[i] = Character.MIN_VALUE;//
                }
            }
            string = newString;
        }
        //TODO: we might need to scale this UUID by Long.MAX_VALUE to ensure a more even distribution (right now buckets might clump toward ASCII chars)
        ByteBuffer bb = ByteBuffer.wrap(string);//convert to long
        return new UUID(new Long(bb.getLong()), 0L);
    }

}

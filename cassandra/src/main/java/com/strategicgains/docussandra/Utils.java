package com.strategicgains.docussandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import com.mongodb.DBObject;
import com.strategicgains.docussandra.cache.CacheFactory;
import com.strategicgains.docussandra.domain.FieldDataType;
import com.strategicgains.docussandra.domain.Identifier;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.IndexField;
import com.strategicgains.docussandra.domain.IndexIdentifier;
import com.strategicgains.docussandra.exception.IndexParseException;
import com.strategicgains.docussandra.exception.IndexParseFieldException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of public static helper methods for various docussandra related
 * tasks.
 *
 * @author udeyoje
 * @since Feb 12, 2015
 */
public class Utils
{

    private static Logger logger = LoggerFactory.getLogger(Utils.class);
    //TODO: ugly copy and paste; fix
    public static final String EMPTY_STRING = "";

    /**
     * Calculates the getIndexName of an iTable based on the dataBaseName, the
     * getTableName, and the getIndexName.
     *
     * Note: No null checks.
     *
     * @param databaseName database getIndexName for the iTable.
     * @param tableName setTable getIndexName for the iTable.
     * @param indexName index getIndexName for the iTable.
     *
     * @return The getIndexName of the iTable for that index.
     */
    public static String calculateITableName(String databaseName, String tableName, String indexName)
    {
        String key = databaseName + ":" + tableName + ":" + indexName;
        Cache c = CacheFactory.getCache("iTableName");
//        synchronized (CacheSynchronizer.getLockingObject(key, "iTableName"))
//        {
        Element e = c.get(key);
        if (e == null || e.getObjectValue() == null)//if its not set, or set, but null, re-read
        {
            //not cached; let's create it
            StringBuilder sb = new StringBuilder();
            sb.append(databaseName);
            sb.append('_');
            sb.append(tableName);
            sb.append('_');
            sb.append(indexName);
            //return sb.toString().toLowerCase();
            e = new Element(key, sb.toString().toLowerCase());
            c.put(e);
        }
        return (String) e.getObjectValue();
//        }
    }

    /**
     * Calculates the getIndexName of an iTable based on an index.
     *
     * Note: No null checks.
     *
     * @param index Index whose iTable getIndexName you would like.
     * @return The getIndexName of the iTable for that index.
     */
    public static String calculateITableName(Index index)
    {
        return calculateITableName(index.getDatabaseName(), index.getTableName(), index.getName());
    }

    /**
     * Calculates the getIndexName of an iTable based on the IDENTIFIER of an
     * index.
     *
     * Note: No null checks.
     *
     * @param indexId index id whose iTable getIndexName you would like.
     * @return The getIndexName of the iTable for that index.
     */
    //TODO: write test for this
    public static String calculateITableName(IndexIdentifier indexId)
    {
        return calculateITableName(indexId.getDatabaseName(), indexId.getTableName(), indexId.getIndexName());
    }

    //TODO: ditch the UUID type; we are misusing it here and another type (probably a string or long) would be more approprate 
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

    /**
     * Creates the database based off of a passed in CQL file. WARNING: Be
     * careful, this could erase data if you are not cautious. Ignores comment
     * lines (lines that start with "//").
     *
     * @param cqlPath path to the CQl file you wish to use to init the database.
     * @param session Database session
     *
     * @throws IOException if it can't read from the CQL file for some reason.
     */
    public static void initDatabase(String cqlPath, Session session) throws IOException
    {
        logger.warn("Initing database from CQL file: " + cqlPath);
        InputStream cqlStream = Utils.class.getResourceAsStream(cqlPath);
        String cql = IOUtils.toString(cqlStream);
        String[] statements = cql.split("\\Q;\\E");
        for (String statement : statements)
        {
            statement = statement.trim();
            statement = statement.replaceAll("\\Q\n\\E", " ");
            if (!statement.equals("") && !statement.startsWith("//"))//don't count comments
            {
                logger.info("Executing CQL statement: " + statement);
                session.execute(statement);
            }
        }
    }

    /**
     * Converts a list to a human readable string.
     *
     * @param list List to convert to a String
     * @return A String that represents the passed in list.
     */
    public static String listToString(List<String> list)//TODO: consider moving to a common lib
    {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String s : list)
        {
            if (!first)
            {
                sb.append(", ");
            } else
            {
                first = false;
            }
            sb.append(s);
        }
        return sb.toString();
    }

    public static void setField(String value, IndexField fieldData, BoundStatement bs, int index) throws IndexParseException
    {
        try
        {
            if (value == null)
            {
                bs.setToNull(index);
            } else if (fieldData.getType().equals(FieldDataType.BINARY))
            {
                bs.setBytes(index, ParseUtils.parseBase64StringAsByteBuffer(value));
            } else if (fieldData.getType().equals(FieldDataType.BOOLEAN))
            {
                bs.setBool(index, ParseUtils.parseStringAsBoolean(value));
            } else if (fieldData.getType().equals(FieldDataType.DATE_TIME))
            {
                bs.setDate(index, ParseUtils.parseStringAsDate(value));
            } else if (fieldData.getType().equals(FieldDataType.DOUBLE))
            {
                bs.setDouble(index, ParseUtils.parseStringAsDouble(value));
            } else if (fieldData.getType().equals(FieldDataType.INTEGER))
            {
                bs.setInt(index, ParseUtils.parseStringAsInt(value));
            } else if (fieldData.getType().equals(FieldDataType.TEXT))
            {
                bs.setString(index, value);
            } else if (fieldData.getType().equals(FieldDataType.UUID))
            {
                bs.setUUID(index, ParseUtils.parseStringAsUUID(value));
            } else
            {
                throw new IndexParseFieldException(fieldData.getField(), new Exception(fieldData.getType().toString() + " is an unsupported type. Please contact support."));
            }
        } catch (IndexParseFieldException parseException)
        {
            throw new IndexParseException(fieldData, parseException);
        }
    }

    /**
     * Sets a field into a BoundStatement.
     *
     * @param jsonObject Object to pull the value from
     * @param fieldData Object describing the field to pull the value from.
     * @param bs BoundStatement to set the field value to.
     * @param index Index in the BoundStatement to set.
     * @return false if the field is null and should cause this bound statement
     * not to be entered into the batch, true otherwise (normal)
     * @throws IndexParseException If there is a problem parsing the field that
     * indicates the entire document should not be indexed.
     */
    public static boolean setField(DBObject jsonObject, IndexField fieldData, BoundStatement bs, int index) throws IndexParseException
    {
        Object jObject = jsonObject.get(fieldData.getField());
        String jsonValue = null;
        if (jObject != null)
        {
            jsonValue = jObject.toString();
        }
        if (jsonValue == null)
        {
            /*
             we can't index on this field, it is null, so we just won't create
             an index on THIS FIELD throw an exception indicating this (just 
             don't add this to the batch)
             */
            return false;
        } else if (jsonValue.isEmpty() && !fieldData.getType().equals(FieldDataType.TEXT))
        {   /*
             if we have an empty string for a non-text field by definition, this
             means that we can't parse it into a useful non-text value so there's
             nothing we can do to index this document, and we can't ignore it
             because this indicates that the field isn't in the expected format, 
             so we need to throw an exception and not index this document AT ALL
             */

            throw new IndexParseException(fieldData, new IndexParseFieldException(jsonValue));
        } else
        {
            //nothing odd here; set the field
            setField(jsonValue, fieldData, bs, index);
            return true;
        }
    }

    public static String join(String delimiter, Object... objects)
    {
        return join(delimiter, Arrays.asList(objects));
    }

    public static String join(String delimiter, Collection<? extends Object> objects)
    {
        if (objects == null || objects.isEmpty())
        {
            return EMPTY_STRING;
        }
        Iterator<? extends Object> iterator = objects.iterator();
        StringBuilder builder = new StringBuilder();
        builder.append(iterator.next());
        while (iterator.hasNext())
        {
            builder.append(delimiter).append(iterator.next());
        }
        return builder.toString();
    }

    //TODO: move this to common a library
    public static boolean equalLists(List<String> one, List<String> two)
    {
        if (one == null && two == null)
        {
            return true;
        }
        if ((one == null && two != null) || one != null && two == null || one.size() != two.size())
        {
            return false;
        }
        ArrayList<String> oneCopy = new ArrayList<>(one);
        ArrayList<String> twoCopy = new ArrayList<>(two);
        Collections.sort(oneCopy);
        Collections.sort(twoCopy);
        return one.equals(twoCopy);
    }

}

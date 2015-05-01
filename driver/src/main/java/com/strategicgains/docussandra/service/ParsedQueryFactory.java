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
package com.strategicgains.docussandra.service;

import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.cache.CacheFactory;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.ParsedQuery;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.domain.WhereClause;
import com.strategicgains.docussandra.exception.FieldNotIndexedException;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.helper.PreparedStatementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating ParsedQueries.
 *
 * @author udeyoje
 */
public class ParsedQueryFactory
{

    private static final Logger logger = LoggerFactory.getLogger(PreparedStatementFactory.class);


    /**
     * Gets a parsed query for the passed in parameters. If the ParsedQuery
     * requested has already been created on this app node, it will retrieve it
     * from a cache instead of recreating it. Use this method instead of
     * parseQuery if a cached copy is acceptable (almost always).
     *
     * @param db Database that the query will run against
     * @param toParse Query to be parsed.
     * @param session Database session.
     * @return a PreparedStatement to use.
     */
    public static ParsedQuery getParsedQuery(String db, Query toParse, Session session)
    {

        if (db == null || db.trim().equals(""))
        {
            throw new IllegalArgumentException("Query must be populated.");
        }
        if (toParse == null)
        {
            throw new IllegalArgumentException("Query cannot be null.");
        }
        final String key = db + ":" + toParse.getTable() + ":" + toParse.getWhere();
        //StopWatch pull = new StopWatch();
        //pull.start();
        Cache c = CacheFactory.getCache("parsedQuery");
//        synchronized (CacheSynchronizer.getLockingObject(key, ParsedQuery.class))
//        {
        Element e = c.get(key);
        //pull.stop();
        //logger.debug("Time to pull a parsed query from cache: " + pull.getTime());
        if (e == null)
        {
            logger.debug("Creating new ParsedQuery for: " + key);
            //StopWatch sw = new StopWatch();
            //sw.start();
            e = new Element(key, parseQuery(db, toParse, session));
            c.put(e);
            //sw.stop();
            //logger.debug("Time to create a new parsed query: " + sw.getTime());
        } else
        {
            logger.trace("Pulling ParsedQuery from Cache: " + e.getObjectValue().toString());
        }
        return (ParsedQuery) e.getObjectValue();
        //}
    }

    /**
     * Parses a query to determine if it is valid and determine the information
     * we actually need to perform the query.
     *
     * @param db Database that the query will run against
     * @param toParse Query to be parsed.
     * @param session Database session.
     * @return A ParsedQuery object for the query.
     * @throws FieldNotIndexedException
     */
    public static ParsedQuery parseQuery(String db, Query toParse, Session session) throws FieldNotIndexedException
    {
        //let's parse the where clause so we know what we are actually searching for
        WhereClause where = new WhereClause(toParse.getWhere());
        //determine if the query is valid; in other words is it searching on valid fields that we have indexed
        List<String> fieldsToQueryOn = where.getFields();
        IndexRepository indexRepo = new IndexRepository(session);
        List<Index> indices = indexRepo.readAllCached(db, toParse.getTable());
        Index indexToUse = null;
        for (Index index : indices)
        {
//            if (index.isActive())//only use active indexes
//            {
                if (equalLists(index.fieldsValues(), fieldsToQueryOn))
                {
                    indexToUse = index;//we have a perfect match; the index matches the query exactly
                    break;
                }
//            }
        }
        if (indexToUse == null)
        {//whoops, no perfect match, let try for a partial match (ie, the index has more fields than the query)
            //TODO: querying on non-primary fields will lead to us being unable to determine which bucket to search -- give the user an override option?, but for now just throw an exception
            for (Index index : indices)
            {
//                if (index.isActive())//only use active indexes
//                {
                    //make a copy of the fieldsToQueryOn so we don't mutate the orginal
                    ArrayList<String> fieldsToQueryOnCopy = new ArrayList<>(fieldsToQueryOn);
                    ArrayList<String> indexFields = new ArrayList<>(index.fieldsValues());//make a copy here too
                    fieldsToQueryOnCopy.removeAll(indexFields);//we remove all the fields we have, from the fields we want
                    //if there are not any fields left in fields we want
                    if (fieldsToQueryOnCopy.isEmpty() && fieldsToQueryOn.contains(indexFields.get(0)))
                    {//second clause in this statement is what ensure we have a primary index; see TODO above.
                        //we have an index that will work (even though we have extra fields in it)
                        indexToUse = index;
                        break;
                    }
//                }
            }
        }
        if (indexToUse == null)
        {
            throw new FieldNotIndexedException(fieldsToQueryOn);
        }
        ParsedQuery toReturn = new ParsedQuery(toParse, where, indexToUse);
        return toReturn;
    }

    //TODO: move this to PearsonLibrary
    public static boolean equalLists(List<String> one, List<String> two)
    {
        if (one == null && two == null)
        {
            return true;
        }

        if ((one == null && two != null)
                || one != null && two == null
                || one.size() != two.size())
        {
            return false;
        }

        //to avoid messing the order of the lists we will use a copy
        ArrayList<String> oneCopy = new ArrayList<>(one);
        ArrayList<String> twoCopy = new ArrayList<>(two);

        Collections.sort(oneCopy);
        Collections.sort(twoCopy);
        return one.equals(twoCopy);
    }
}

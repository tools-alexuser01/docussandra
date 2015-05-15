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
package com.strategicgains.docussandra.persistence.helper;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.cache.CacheFactory;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating and reusing PreparedStatements.
 *
 * @author udeyoje
 */
public class PreparedStatementFactory
{
    //private static Cache preparedStatementCache = null;
    // private static boolean established = false;

    private static final Logger logger = LoggerFactory.getLogger(PreparedStatementFactory.class);

    private static final Object LOCK = new Object();

//    /**
//     * Establishes the cache if it doesn't exist.
//     */
//    private synchronized static void establishCache()
//    {     
//        logger.debug("Establishing prepared statement cache...");
//        if (preparedStatementCache == null)
//        {
//            preparedStatementCache = CacheFactory.getCache("preparedStatements");
//        }
//        established = true;
//    }
    /**
     * Gets a prepared statement. Could be new, or from the cache.
     *
     * @param query The query to get the statement for.
     * @param session The session to create the statement in.
     * @return a PreparedStatement to use.
     */
    public static PreparedStatement getPreparedStatement(String query, Session session)
    {
//        if (!established)
//        {
//            establishCache();
//        }
        //StopWatch sw = new StopWatch();
        //sw.start();
        if (query == null || query.trim().equals(""))
        {
            throw new IllegalArgumentException("Query must be populated.");
        }
        if (session == null)
        {
            throw new IllegalArgumentException("Session cannot be null.");
        }
        query = query.trim();
        Cache c = CacheFactory.getCache("preparedStatements");
        Element e = null;
        synchronized (LOCK)
        {
            e = c.get(query);
            if (e == null || e.getObjectValue() == null)
            {
                logger.debug("Creating new Prepared Statement for: " + query);
                e = new Element(query, session.prepare(query));
                c.put(e);
            } else
            {
                if (logger.isTraceEnabled())
                {
                    PreparedStatement ps = (PreparedStatement) e.getObjectValue();
                    logger.trace("Pulling PreparedStatement from Cache: " + ps.getQueryString());
                }
            }
        }
        //sw.stop();
        //logger.debug("Time to fetch prepared statement (" + query + "): " + sw.getTime());
        return (PreparedStatement) e.getObjectValue();

    }

}

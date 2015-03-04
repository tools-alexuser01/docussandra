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
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

/**
 * Factory for creating and reusing prepared statements.
 *
 * @author udeyoje
 */
public class PreparedStatementFactory
{

    private static CacheManager manager = null;
    private static Cache preparedStatementCache = null;
    private static boolean established = false;

    /**
     * Establishes the cache if it doesn't exist.
     */
    private synchronized static void establishCache()
    {
        if (manager == null)
        {
            manager = CacheManager.newInstance("./src/main/resources/ehcache.xml");
        }
        if (preparedStatementCache == null)
        {
            preparedStatementCache = manager.getCache("preparedStatements");
        }
        established = true;

    }

    /**
     * Gets a prepared statement. Could be new, or from the cache.
     *
     * @param query The query to get the statement for.
     * @param session The session to create the statement in.
     * @return a PreparedStatement to use.
     */
    public static PreparedStatement getPreparedStatement(String query, Session session)
    {
        if (!established)
        {
            establishCache();
        }
        if (query == null || query.trim().equals(""))
        {
            throw new IllegalArgumentException("Query must be populated.");
        }
        if (session == null)
        {
            throw new IllegalArgumentException("Session cannot be null.");
        }
        query = query.trim();
        Element e = preparedStatementCache.get(query);
        if (e == null)
        {
            e = new Element(query, session.prepare(query));
            preparedStatementCache.put(e);
        }
        return (PreparedStatement) e.getObjectValue();
    }

    /**
     * Shuts down the cache; only call upon application shutdown.
     */
    public static void shutdownCache()
    {
        if (manager != null)
        {
            manager.shutdown();
        }
    }

}

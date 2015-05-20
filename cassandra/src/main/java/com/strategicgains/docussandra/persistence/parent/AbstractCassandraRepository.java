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
package com.strategicgains.docussandra.persistence.parent;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.Identifier;

/**
 * Super class for our repositories.
 *
 * @author udeyoje
 */
public abstract class AbstractCassandraRepository
{

    /**
     * A pre-configured Session instance.
     */
    private Session session;
    /**
     * The name of the Cassandra table entities are stored in.
     */
    private String table;

    /**
     * Default constructor.
     */
    public AbstractCassandraRepository()
    {

    }

    /**
     * Constructor.
     *
     * @param session a pre-configured Session instance.
     * @param tableName the name of the Cassandra table entities are stored in.
     */
    public AbstractCassandraRepository(Session session, String tableName)
    {
        this.session = session;
        this.table = tableName;
    }

    /**
     * Constructor.
     *
     * @param session a pre-configured Session instance.
     */
    public AbstractCassandraRepository(Session session)
    {
        this.session = session;
    }

    /**
     * Gets the database session.
     *
     * @return
     */
    protected Session getSession()
    {
        return session;
    }

    /**
     * Gets the database table.
     *
     * @return
     */
    protected String getTable()
    {
        return table;
    }

    /**
     * Binds an identifier object.
     *
     * @param bs
     * @param identifier
     */
    protected void bindIdentifier(BoundStatement bs, Identifier identifier)
    {
        bs.bind(identifier.components().toArray());
    }

}

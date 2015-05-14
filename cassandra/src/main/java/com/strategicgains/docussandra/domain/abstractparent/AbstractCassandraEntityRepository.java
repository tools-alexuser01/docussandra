/*
 Copyright 2013, Strategic Gains, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
package com.strategicgains.docussandra.domain.abstractparent;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.Identifier;
import com.strategicgains.docussandra.persistence.abstractparent.AbstractCassandraRepository;
import com.strategicgains.docussandra.persistence.helper.PreparedStatementFactory;

/**
 * A Cassandra repository that manages types of Identifiable instances, which
 * are identified by a single primary key.
 * <p/>
 * Extend this repository to persist Identifiable instances that have a single,
 * unique identifier, that is not a UUID and you don't need the createdAt and
 * updatedAt time stamps (of TimestampedIdentifiable) automatically applied.
 *
 * @author toddf
 * @since Apr 12, 2013
 */
public abstract class AbstractCassandraEntityRepository<T extends Identifiable> extends AbstractCassandraRepository
{

    private static final String EXISTENCE_CQL = "select count(*) from %s where %s = ?";
    private static final String READ_CQL = "select * from %s where %s = ?";
    private static final String DELETE_CQL = "delete from %s where %s = ?";

    private String identifierColumn;
    private PreparedStatement existStmt;
    private PreparedStatement readStmt;
    protected PreparedStatement deleteStmt;

    /**
     * @param session a pre-configured Session instance.
     * @param tableName the name of a table this repository works against.
     * @param identifierColumn the column name that holds the row key or unique
     * identifier.
     */
    public AbstractCassandraEntityRepository(Session session, String tableName, String identifierColumn)
    {
        super(session, tableName);
        this.identifierColumn = identifierColumn;
        initialize();
    }

    protected void initialize()
    {
        existStmt = PreparedStatementFactory.getPreparedStatement(String.format(EXISTENCE_CQL, getTable(), identifierColumn), getSession());
        readStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_CQL, getTable(), identifierColumn), getSession());
        deleteStmt = PreparedStatementFactory.getPreparedStatement(String.format(DELETE_CQL, getTable(), identifierColumn), getSession());
    }

    public String getIdentifierColumn()
    {
        return identifierColumn;
    }

    //@Override
    public boolean exists(Identifier identifier)
    {
        if (identifier == null || identifier.isEmpty())
        {
            return false;
        }

        BoundStatement bs = new BoundStatement(existStmt);
        bs.bind(identifier.getComponentAsString(0));
        return (getSession().execute(bs).one().getLong(0) > 0);
    }

    //@Override
    public T readEntityById(Identifier identifier)
    {
        if (identifier == null || identifier.isEmpty())
        {
            return null;
        }

        BoundStatement bs = new BoundStatement(readStmt);
        bs.bind(identifier.getComponentAsString(0));
        return marshalRow(getSession().execute(bs).one());
    }

    //@Override
    public void deleteEntity(T entity)
    {
        if (entity == null)
        {
            return;
        }
        deleteEntityById(entity.getId());
    }

    public void deleteEntityById(Identifier identifier)
    {
        if (identifier == null)
        {
            return;
        }

        BoundStatement bs = new BoundStatement(deleteStmt);
        bindIdentifier(bs, identifier);
        getSession().execute(bs);
    }

    protected abstract T marshalRow(Row row);
}

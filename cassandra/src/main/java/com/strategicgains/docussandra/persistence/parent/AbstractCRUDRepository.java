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

import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.Identifier;
import com.strategicgains.docussandra.domain.parent.Identifiable;
import java.util.List;

/**
 * Abstract class for our CRUD repositories. Mainly just to enforce naming
 * conventions for right now.
 *
 * @author udeyoje
 * @param <T> Object type for this repo.
 */
public abstract class AbstractCRUDRepository<T extends Identifiable> extends AbstractCassandraRepository
{

    /**
     * @param session a pre-configured Session instance.
     * @param tableName the name of the Cassandra table entities are stored in.
     */
    public AbstractCRUDRepository(Session session, String tableName)
    {
        super(session, tableName);
    }

    /**
     * @param session a pre-configured Session instance.
     */
    public AbstractCRUDRepository(Session session)
    {
        super(session);
    }

    public abstract T create(T entity);

    public abstract T update(T entity);

    public List<T> readAll()
    {
        throw new UnsupportedOperationException("Not valid for this class.");
    }

    public List<T> readAll(Identifier id)
    {
        throw new UnsupportedOperationException("Not valid for this class.");
    }

    public abstract boolean exists(Identifier id);

    public abstract T read(Identifier id);

    public abstract void delete(T entity);

    public abstract void delete(Identifier id);
}

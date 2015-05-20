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
package com.strategicgains.docussandra.persistence;

import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.Identifier;
import com.strategicgains.docussandra.event.IndexCreatedEvent;
import java.util.List;
import java.util.UUID;

/**
 *
 * @author udeyoje
 */
public interface IndexStatusRepository
{

    /**
     * Creates an index status in the db.
     *
     * @param entity IndexCreatedEvent to store as a status in the DB.
     * @return The created IndexCreatedEvent.
     */
    IndexCreatedEvent create(IndexCreatedEvent entity);

    void delete(Identifier id);

    void delete(IndexCreatedEvent entity);

    /**
     * Checks to see if an IndexCreatedEvent exists by UUID.
     *
     * @param uuid Index status UUID to check for existence.
     * @return True if the index status exists, false if it does not.
     */
    boolean exists(UUID uuid);

    boolean exists(Identifier id);

    Session getSession();

    /**
     * Reads an IndexCreatedEvent by UUID.
     *
     * @param uuid UUID to fetch.
     * @return an IndexCreatedEvent for that UUID.
     */
    IndexCreatedEvent read(UUID uuid);

    IndexCreatedEvent read(Identifier id);

    /**
     * Reads all IndexCreatedEvents. This is quite possibly a very expensive
     * operation; use with care.
     *
     * @return All IndexCreatedEvents that the database has a record of.
     */
    List<IndexCreatedEvent> readAll();

    List<IndexCreatedEvent> readAll(Identifier id);

    /**
     * Gets all IndexCreatedEvents that are currently indexing. This method is
     * preferred to readAll(). This provides a sense of the indexing load on the
     * database.
     *
     * @return All currently indexing IndexCreatedEvents.
     */
    List<IndexCreatedEvent> readAllCurrentlyIndexing();

    /**
     * Updates the status for an IndexCreatedEvent in the database. Take note:
     * only a few getFields are updatable: numberOfRecordsCompleted,
     * statusLastUpdatedAt, and error. This is a logical decision; there should
     * not be a reason to update any other fields. This will also mark the
     * record as done indexing or not as appropriate.
     *
     * @param entity The IndexCreatedEvent to update with the proper fields
     * set.
     * @return The updated IndexCreatedEvent.
     */
    IndexCreatedEvent update(IndexCreatedEvent entity);

}

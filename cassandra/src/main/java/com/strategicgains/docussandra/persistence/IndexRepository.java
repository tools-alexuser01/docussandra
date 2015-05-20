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

import com.strategicgains.docussandra.domain.Identifier;
import com.strategicgains.docussandra.domain.Index;
import java.util.List;

/**
 *
 * @author udeyoje
 */
public interface IndexRepository
{

    long countAll(Identifier id);

    Index create(Index entity);

    void delete(Identifier id);

    void delete(Index entity);

    boolean exists(Identifier identifier);

    /**
     * Marks an index as "active" meaning that indexing has completed on it.
     *
     * @param entity Index to mark active.
     */
    void markActive(Index entity);

    Index read(Identifier identifier);

    List<Index> readAll(Identifier id);

    List<Index> readAll();

    /**
     * Same as readAll, but will read from the cache if available.
     *
     * @return
     */
    List<Index> readAllCached(Identifier id);

    Index update(Index entity);
    
}

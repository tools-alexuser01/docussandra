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

import com.strategicgains.docussandra.domain.Identifier;
import com.strategicgains.docussandra.domain.parent.Identifiable;
import java.util.List;

/**
 * Interface for our repositories. Mainly just to enforce naming conventions for
 * right now.
 *
 * @author udeyoje
 * @param <T> Object type for this repo.
 */
public interface RepositoryInterface<T extends Identifiable>
{

    public T create(T entity);

    public T update(T entity);

    public List<T> readAll();

    public List<T> readAll(Identifier id);

    public boolean exists(Identifier id);

    public T read(Identifier id);

    public void delete(T entity);

    public void delete(Identifier id);
}

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

import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Identifier;
import java.util.List;

/**
 *
 * @author udeyoje
 */
public interface DatabaseRepository
{

    Database create(Database entity);

    void delete(Database entity);

    void delete(Identifier identifier);

    boolean exists(Identifier identifier);

    Database read(Identifier identifier);

    List<Database> readAll();

    List<Database> readAll(Identifier id);

    Database update(Database entity);

}

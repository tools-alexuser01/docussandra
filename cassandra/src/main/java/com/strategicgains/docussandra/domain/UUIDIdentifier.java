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
package com.strategicgains.docussandra.domain;

import java.util.UUID;

/**
 * Identifier for just using UUIDs.
 *
 * @author udeyoje
 */
public class UUIDIdentifier extends Identifier
{

    /**
     * Constructor.
     *
     * @param id
     */
    public UUIDIdentifier(UUID id)
    {
        super.add(id);
    }

    /*
     * Constructor.
     * @param id 
     */
    public UUIDIdentifier(UUIDIdentifier id)
    {
        super(id);
    }

    /**
     * Do not call. Not valid for this class. Will throw
     * UnsupportedOperationException.
     *
     * @return
     */
    @Override
    public String getDatabaseName()
    {
        throw new UnsupportedOperationException("Not a valid call for this class. UUID Identifiers do not have an associated database.");
    }

    /**
     * Do not call. Not valid for this class. Will throw
     * UnsupportedOperationException.
     *
     * @return
     */
    @Override
    public String getTableName()
    {
        throw new UnsupportedOperationException("Not a valid call for this class. UUID Identifiers do not have an associated table.");
    }

    /**
     * Do not call. Not valid for this class. Will throw
     * UnsupportedOperationException.
     *
     * @return
     */
    @Override
    public Table getTable()
    {
        throw new UnsupportedOperationException("Not a valid call for this class. UUID Identifiers do not have an associated table.");
    }

    /**
     * Gets the UUID associated with this identifier.
     *
     * @return
     */
    public UUID getUUID()
    {
        return (UUID) super.components().get(0);
    }
}

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
 * Supports the concept of a compound identifier. An Identifier is made up of
 * components, which are Object instances. The components are kept in order of
 * which they are added.
 *
 * @author toddf
 * @since Aug 29, 2013
 */
public class DocumentIdentifier extends Identifier
{

    public DocumentIdentifier(Identifier id)
    {
        super(id.components().toArray());
    }
//TODO: javadoc
    public UUID getUUID()
    {
        if (super.size() >= 3)
        {
            return (UUID) super.getComponent(2);
        }
        return null;
    }
}

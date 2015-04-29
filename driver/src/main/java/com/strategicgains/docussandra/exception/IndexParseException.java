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
package com.strategicgains.docussandra.exception;

import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.IndexField;

/**
 * Exception that indicates that a field that should be indexable is not in the specified format.
 * @author udeyoje
 */
public class IndexParseException extends RuntimeException
{

    /**
     * Field that could not be indexed in the document.
     */
    private IndexField field;
    /**
     * Document that could not be indexed.
     */
    private Document entity;
    
    /**
     * Constructor.
     * @param entity Document that could not be indexed.
     * @param field Field that could not be indexed in the document.
     */
    public IndexParseException(IndexField field, Document entity)
    {
        super("The field: " + field.toString() + " could not be parsed from the document: " + entity.toString());        
        this.entity = entity;
        this.field = field;
    }

    /**
     * Field that could not be indexed in the document.
     * @return the field
     */
    public IndexField getField()
    {
        return field;
    }

    /**
     * Document that could not be indexed.
     * @return the entity
     */
    public Document getEntity()
    {
        return entity;
    }
}

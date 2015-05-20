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

import com.strategicgains.docussandra.domain.IndexField;

/**
 * Exception that indicates that a field that should be indexable is not in the
 * specified format. Contains more meta-data than using an
 * IndexParseFieldException alone.
 *
 * @author udeyoje
 */
public class IndexParseException extends IndexParseFieldException
{

    /**
     * Field that could not be indexed in the document.
     */
    private IndexField field;
//    /**
//     * Document that could not be indexed.
//     */
//    private Document entity;

    /**
     * Constructor.
     *
     * @param field Field that could not be indexed in the document.
     * @param parent Parent class to use.
     */
    public IndexParseException(IndexField field, IndexParseFieldException parent)
    {
        super("The field: " + field.toString() + " could not be parsed from the document or query, it is not a valid value for the specified datatype.", parent.getFieldValue(), parent.getCause());
        this.field = field;
    }

    /**
     * Field that could not be indexed in the document.
     *
     * @return the field
     */
    public IndexField getField()
    {
        return field;
    }

    @Override
    public String toString()
    {
        return "IndexParseException{" + "field=" + field + ", fieldValue=" + super.getFieldValue() + '}';
    }

}

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
package com.strategicgains.docussandra.domain;

import com.strategicgains.syntaxe.Validatable;
import com.strategicgains.syntaxe.ValidationException;
import com.strategicgains.syntaxe.annotation.Required;
import java.util.ArrayList;
import java.util.List;

/**
 * Describes a field that will be indexed.
 *
 * @author udeyoje
 */
public class IndexField implements Validatable
{

    /**
     * String of the field of this field.
     */
    @Required
    private String field;

    /**
     * Flag indicating that it should be index as ascending or descending.
     */
    private boolean isAscending = true;

    /**
     * Type of field that the field represents.
     */
    private FieldDataType type = FieldDataType.TEXT;

    /**
     * Constructor.
     *
     * @param value
     * @param type
     */
    public IndexField(String value, FieldDataType type)
    {
        field = value.trim();

        if (field.trim().startsWith("-"))
        {
            field = value.substring(1);
            isAscending = false;
        }
        this.type = type;
    }

    /**
     * Constructor. Type is defaulted to TEXT.
     *
     * @param value
     */
    public IndexField(String value)
    {
        this.field = value;
        this.type = FieldDataType.TEXT;
    }

    /**
     * String of the field of this field.
     *
     * @return the field
     */
    public String getField()
    {
        return field;
    }

    /**
     * Type of field that the field represents.
     *
     * @return the type
     */
    public FieldDataType getType()
    {
        return type;
    }

    /**
     * Gets if this field is ascending or not.
     *
     * @return
     */
    public boolean isAscending()
    {
        return isAscending;
    }

    @Override
    public void validate()
    {
        final List<String> errors = new ArrayList<>();
        if (field == null || field.isEmpty())
        {
            errors.add("Field is required.");// will probably never happen
        }

        if (type == null)
        {
            errors.add("Field data type is required.");
        }

        if (!errors.isEmpty())
        {
            throw new ValidationException(errors);
        }
    }

}

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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Describes a field that will be indexed.
 *
 * @author udeyoje
 */
public class IndexField implements Validatable, Serializable
{

    private static final Pattern fieldPattern = Pattern.compile("^[\\+-]?\\w+");

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

    public IndexField()
    {
    }

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

        if (!fieldPattern.matcher(field).matches())
        {
            errors.add("Invalid index field name: " + field);
        }
        if (type == null)
        {
            errors.add("Field data type is required.");//this should not happen either, unless someone explicitly sets it to null
        }

        if (!errors.isEmpty())
        {
            throw new ValidationException(errors);
        }
    }

    @Override
    public int hashCode()
    {
        int hash = 3;
        hash = 97 * hash + Objects.hashCode(this.field);
        hash = 97 * hash + (this.isAscending ? 1 : 0);
        hash = 97 * hash + Objects.hashCode(this.type);
        return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }
        final IndexField other = (IndexField) obj;
        if (!Objects.equals(this.field, other.field))
        {
            return false;
        }
        if (this.isAscending != other.isAscending)
        {
            return false;
        }
        if (this.type != other.type)
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return field + "/" + type; //IndexField{" + "field=" + field + ", isAscending=" + isAscending + ", type=" + type + '}';
    }

}

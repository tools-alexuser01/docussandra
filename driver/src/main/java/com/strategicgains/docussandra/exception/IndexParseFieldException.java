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

/**
 * Exception that indicates that a field that should be indexable is not in the
 * specified format. This is a object should be used as a superclass to
 * IndexParseException as this simply will contain more specific data.
 *
 * @author udeyoje
 */
public class IndexParseFieldException extends Exception
{

    /**
     * Value of the field with the problem.
     */
    private String fieldValue;
//
//    /**
//     * Constructors for child classes to call.
//     * @param message Message for this exception.
//     * @param fieldValue Value of the field with the problem.
//     */
//    protected IndexParseFieldException(String message, String fieldValue)
//    {
//        super(message);
//        this.fieldValue = fieldValue;
//    }

    /**
     * Constructors for child classes to call.
     * @param message Message for this exception.
     * @param fieldValue Value of the field with the problem.
     * @param e Root exception for the problem.
     */
    protected IndexParseFieldException(String message, String fieldValue, Throwable e)
    {
        super(message + " Could not parse field with value: '" + fieldValue + "'", e);
        this.fieldValue = fieldValue;
    }   

    /**
     * Constructor.
     *
     * @param fieldValue Value of the field with a problem.
     */
    public IndexParseFieldException(String fieldValue)
    {
        super("Could not parse field with value: '" + fieldValue + "'");
        this.fieldValue = fieldValue;
    }

    /**
     * Constructor.
     *
     * @param e Exception that caused this problem.
     * @param fieldValue Value of the field with a problem.
     */
    public IndexParseFieldException(String fieldValue, Exception e)
    {
        super("Could not parse field. '" + fieldValue + "'", e);
        this.fieldValue = fieldValue;
    }

    /**
     * Value of the field with the problem.
     *
     * @return the fieldValue
     */
    public String getFieldValue()
    {
        return fieldValue;
    }

}

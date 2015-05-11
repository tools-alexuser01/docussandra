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
 * Exception that indicates that an indexed field is listed as "null" in the
 * JSON and therefore cannot be indexed on this field. This is not necessarily
 * (and probably isn't) an error, it just indicates that we should not try to
 * create an insert/update statement based on this field.
 *
 * @author udeyoje
 */
public class NullFieldException extends Exception
{

    public NullFieldException()
    {
        super("Cannot create index for a field that is null.");
    }
}

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

/**
 * Enum for types of data that we can index on.
 * @author udeyoje
 */
public enum FieldDataType
{
    TEXT,
    DATE_TIME,
    DOUBLE,
    INTEGER,
    BOOLEAN,
    UUID,
    BINARY;// maybe?
    
    public static String mapToCassandaraDataType(FieldDataType in){
        if(in.equals(TEXT)){
            return "varchar";
        } else if(in.equals(DATE_TIME)){
            return "timestamp";
        } else if(in.equals(DOUBLE)){
            return "double";
        } else if(in.equals(INTEGER)){
            return "int";//should we use bigint instead?
        } else if(in.equals(BOOLEAN)){
            return "boolean";
        } else if(in.equals(UUID)){
            return "uuid";
        } else if(in.equals(BINARY)){
            return "blob";
        } else {
            throw new IllegalArgumentException("Type not supported. " + in.toString());//this should never happen
        }
    }
    
    
}

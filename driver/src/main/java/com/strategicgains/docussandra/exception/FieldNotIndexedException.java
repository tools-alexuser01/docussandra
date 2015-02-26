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

import java.util.List;

/**
 * Exception that indicates an attempted query on a field that is not indexed.
 *
 * @author udeyoje
 */
public class FieldNotIndexedException extends RuntimeException {

    /**
     * Field that an index does not exist for.
     */
    private List<String> fields;

    /**
     * Constructor.
     * @param fields List of fields of which at least one does not exist in a known index.
     */
    public FieldNotIndexedException(List<String> fields) {
        super("One of the following fields: [" + listToString(fields) + "] does not exist in any known indices. Try adding an index (if you understand the ramifications of this).");
        this.fields = fields;
    }
    
    private static String listToString(List<String> list){
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String s : list){
            if(!first){
                sb.append(", " );
            } else {
                first = false;
            }
            sb.append(s);
        }
        return sb.toString();
    }

    /**
     * Fields that at least one of which does not exist an index.
     *
     * @return
     */
    public List<String> getFields() {
        return fields;
    }
}

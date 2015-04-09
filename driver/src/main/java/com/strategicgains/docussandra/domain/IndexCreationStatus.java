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

import java.util.Date;

/**
 * POJO that contains the current status of an indexing action.
 * @author udeyoje
 */
public class IndexCreationStatus
{
    /**
     * The date that this request to make an index was issued.
     */
    private Date dateStarted;
        
    /**
     * Estimated time to completion of this index.
     */
    private long eta;//likely a pipe dream, but it would be sweet if this worked
    
    /**
     * The requested index that is being created.
     */
    private Index index;
    
    
    
//    public UUID getId(){
//        return index.getId().
//    }
    
    
    public boolean isDone(){
        return index.isIsActive();
    }
}

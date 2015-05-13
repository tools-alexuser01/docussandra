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

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper for returning queries. Contains metadata about the response in
 * addition to the actual response.
 *
 * @author udeyoje
 */
public class QueryResponseWrapper extends ArrayList<Document>
{

    /**
     * Number of additional results that exist. Null if there are additional
     * results, but the number is unknown.
     */
    private final Long numAdditionalResults;

    /**
     * Constructor.
     *
     * @param responseData The actual response data.
     * @param numAdditionalResults Number of additional results that exist. Null
     * if there are additional results, but the number is unknown.
     */
    public QueryResponseWrapper(List<Document> responseData, Long numAdditionalResults)
    {
        super(responseData);
        this.numAdditionalResults = numAdditionalResults;
    }


    /**
     * Number of additional results that exist. Null if there are additional
     * results, but the number is unknown.
     * @return the numAdditionalResults
     */
    public Long getNumAdditionalResults()
    {
        return numAdditionalResults;
    }
    
    

}

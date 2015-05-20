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
package com.strategicgains.docussandra.persistence;

import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.domain.ParsedQuery;
import com.strategicgains.docussandra.domain.QueryResponseWrapper;
import com.strategicgains.docussandra.exception.IndexParseException;

/**
 * Repository for querying for records.
 *
 * @author udeyoje
 */
public interface QueryRepository
{

    /**
     * @return the session
     */
    Session getSession();

    /**
     * Do a query without limit or offset.
     *
     * @param query ParsedQuery to execute.
     * @return A query response.
     * @throws IndexParseException If the query is not on a valid index.
     */
    public QueryResponseWrapper query(ParsedQuery query) throws IndexParseException;

    /**
     * Do a query with limit and offset.
     *
     * @param query ParsedQuery to execute.
     * @param limit Maximum number of results to return.
     * @param offset Number of records at the beginning of the results to
     * discard.
     * @return A query response.
     * @throws IndexParseException If the query is not on a valid index.
     */
    public QueryResponseWrapper query(ParsedQuery query, int limit, long offset) throws IndexParseException;

}

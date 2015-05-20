package com.strategicgains.docussandra.service;

import com.strategicgains.docussandra.domain.ParsedQuery;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.domain.QueryResponseWrapper;
import com.strategicgains.docussandra.exception.FieldNotIndexedException;
import com.strategicgains.docussandra.exception.IndexParseException;
import com.strategicgains.docussandra.persistence.QueryRepository;

/**
 * Service for performing a query.
 *
 * @author udeyoje
 */
public class QueryService
{

    /**
     * Query Repository for accessing the database.
     */
    private QueryRepository queries;

    /**
     * Constructor.
     *
     * @param queryRepository QueryRepositoryImpl to use to perform the query.
     */
    public QueryService(QueryRepository queryRepository)
    {
        super();
        this.queries = queryRepository;
    }

    /**
     * Does a query with no limit or offset.
     *
     * @param db Database to query.
     * @param toQuery Query perform.
     * @return A query response object containing a list of documents and some
     * metadata about the query.
     * @throws FieldNotIndexedException If the field that was attempted to be
     * queried on is not part of an index.
     * @throws IndexParseException If the field that was attempted to be queried
     * on was not in a recognized format.
     */
    public QueryResponseWrapper query(String db, Query toQuery) throws IndexParseException, FieldNotIndexedException
    {
        ParsedQuery parsedQuery = ParsedQueryFactory.getParsedQuery(db, toQuery, queries.getSession());
        return queries.query(parsedQuery);
    }

    /**
     * Does a query with limit and offset.
     *
     * @param db Database to query.
     * @param toQuery Query perform.
     * @param limit max number of results to return
     * @param offset offset of the query results
     * @return A query response object containing a list of documents and some
     * metadata about the query.
     * @throws FieldNotIndexedException If the field that was attempted to be
     * queried on is not part of an index.
     * @throws IndexParseException If the field that was attempted to be queried
     * on was not in a recognized format.
     */
    public QueryResponseWrapper query(String db, Query toQuery, int limit, long offset) throws IndexParseException, FieldNotIndexedException
    {
        ParsedQuery parsedQuery = ParsedQueryFactory.getParsedQuery(db, toQuery, queries.getSession());
        return queries.query(parsedQuery, limit, offset);
    }
}

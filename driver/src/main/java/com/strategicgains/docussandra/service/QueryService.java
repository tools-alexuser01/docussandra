package com.strategicgains.docussandra.service;

import com.strategicgains.docussandra.domain.ParsedQuery;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.domain.QueryResponseWrapper;
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
     * @param queryRepository QueryRepository to use to perform the query.
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
     */
    public QueryResponseWrapper query(String db, Query toQuery)
    {
        ParsedQuery parsedQuery = ParsedQueryFactory.getParsedQuery(db, toQuery, queries.getSession());//note: throws a runtime exception
        return queries.doQuery(parsedQuery);
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
     */
    public QueryResponseWrapper query(String db, Query toQuery, int limit, long offset)
    {
        ParsedQuery parsedQuery = ParsedQueryFactory.getParsedQuery(db, toQuery, queries.getSession());//note: throws a runtime exception
        return queries.doQuery(parsedQuery, limit, offset);
    }

//
//	public Query create(Query entity)
//	{
//		ValidationEngine.validateAndThrow(entity);
//		return queries.create(entity);
//	}
//
//	public Query read(Identifier id)
//    {
//		return queries.read(id);
//    }
//
//	public void update(Query entity)
//    {
//		ValidationEngine.validateAndThrow(entity);
//		queries.update(entity);
//    }
//
//	public void delete(Identifier id)
//    {
//		queries.delete(id);
//    }
}

package com.strategicgains.docussandra.controller;

import org.restexpress.Request;
import org.restexpress.Response;

import com.strategicgains.docussandra.Constants;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.service.QueryService;
import com.strategicgains.hyperexpress.builder.UrlBuilder;
import java.util.List;
import org.restexpress.common.query.QueryRange;
import org.restexpress.query.QueryRanges;

/**
 * This is the 'controller' layer, where HTTP details are converted to domain
 * concepts and passed to the service layer. Then service layer response
 * information is enhanced with HTTP details, if applicable, for the response.
 * <p/>
 * This controller demonstrates how to process a Cassandra entity that is
 * identified by a single, primary row key such as a UUID.
 */
public class QueryController
{

    private static final UrlBuilder LOCATION_BUILDER = new UrlBuilder();
    private static final int DEFAULT_LIMIT = 20;

    private QueryService service;

    public QueryController(QueryService queryService)
    {
        super();
        this.service = queryService;
    }

    public List<Document> query(Request request, Response response)
    {
        String database = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String table = request.getHeader(Constants.Url.TABLE, "No table provided");
        Query toQuery = request.getBodyAs(Query.class, "Query details not provided");
        toQuery.setTable(table);//change of plans, no longer getting it from the query object, but from the URL instead

        QueryRange range = QueryRanges.parseFrom(request);

        int limit = DEFAULT_LIMIT;//TODO: enforce maximum limit
        long offset = 0;
        if (range != null)
        {
            if (range.hasLimit())
            {
                limit = range.getLimit();
            }
            if (range.hasOffset())
            {
                offset = range.getOffset();
            }
        }
        List<Document> queryResponse = service.query(database, toQuery, limit, offset);

        //Document document = documents.read(database, table, new Identifier(database, table, UuidConverter.parse(id)));
        // enrich the entity with links, etc. here...
        //TODO: come back to thisHyperExpress.bind(Constants.Url.DOCUMENT_ID, document.getUuid().toString());
        return queryResponse;
    }
}

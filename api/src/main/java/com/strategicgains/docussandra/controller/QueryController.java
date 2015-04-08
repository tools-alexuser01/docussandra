package com.strategicgains.docussandra.controller;

import org.restexpress.Request;
import org.restexpress.Response;

import com.strategicgains.docussandra.Constants;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.domain.QueryResponseWrapper;
import com.strategicgains.docussandra.service.QueryService;
import com.strategicgains.hyperexpress.builder.UrlBuilder;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.List;
import org.restexpress.common.query.QueryRange;
import org.restexpress.query.QueryRanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final int DEFAULT_LIMIT = 20;
    private Logger logger = LoggerFactory.getLogger(this.getClass());

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
            } else
            {
                range.setLimit(limit);
            }
            if (range.hasOffset())
            {
                offset = range.getOffset();
            } else
            {
                range.setOffset(offset);
            }
        }
        QueryResponseWrapper queryResponse = service.query(database, toQuery, limit, offset);
        if (queryResponse.isEmpty())
        {
            response.setCollectionResponse(range, 0, 0);
        } else if (queryResponse.getNumAdditionalResults() == null)
        {//we have more results, but an unknown number more
            response.setCollectionResponse(range, queryResponse.size(), -1);
            response.setResponseStatus(HttpResponseStatus.PARTIAL_CONTENT);
        } else if (queryResponse.getNumAdditionalResults() == 0l)
        {//we have no more results, the amount returned is the number that exists
            response.setCollectionResponse(range, queryResponse.size(), queryResponse.size());
        } else// not likely to actually happen given our implementation, but coding in case our backend changes
        {
            response.setCollectionResponse(range, queryResponse.size(), queryResponse.size() + queryResponse.getNumAdditionalResults());
        }
        logger.debug("Query: " + toQuery.toString() + " returned " + queryResponse.size() + " documents.");
        //Document document = documents.read(database, table, new Identifier(database, table, UuidConverter.parse(id)));
        // enrich the entity with links, etc. here...
        //TODO: come back to thisHyperExpress.bind(Constants.Url.DOCUMENT_ID, document.getUuid().toString());
        return queryResponse;
    }
}

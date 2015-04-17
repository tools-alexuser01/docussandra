package com.strategicgains.docussandra.controller;

import io.netty.handler.codec.http.HttpMethod;

import java.util.List;

import org.restexpress.Request;
import org.restexpress.Response;

import com.strategicgains.docussandra.Constants;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.IndexCreationStatus;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.service.IndexService;
import com.strategicgains.hyperexpress.HyperExpress;
import com.strategicgains.hyperexpress.builder.TokenBinder;
import com.strategicgains.hyperexpress.builder.TokenResolver;
import com.strategicgains.hyperexpress.builder.UrlBuilder;
import com.strategicgains.repoexpress.domain.Identifier;
import java.util.ArrayList;
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
public class IndexController
{

    private static final UrlBuilder LOCATION_BUILDER = new UrlBuilder();
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexController.class);

    private IndexService indexes;

    public IndexController(IndexService indexService)
    {
        super();
        this.indexes = indexService;
    }

    public IndexCreationStatus create(Request request, Response response)
    {
        String database = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String table = request.getHeader(Constants.Url.TABLE, "No table provided");
        String name = request.getHeader(Constants.Url.INDEX, "No index name provided");
        Index entity = request.getBodyAs(Index.class, "Resource details not provided");
        Table t = new Table();
        t.database(database);
        t.name(table);
        entity.table(t);
        entity.name(name);
        if (entity.includeOnly() == null)
        {
            entity.includeOnly(new ArrayList<String>(0));
        }
        IndexCreationStatus status;
        try
        {
            status = indexes.create(entity);
        } catch (Exception e)
        {
            LOGGER.error("Could not save index", e);
            throw e;
        }
        // Construct the response for create...
        response.setResponseCreated();

        // enrich the resource with links, etc. here...
        TokenResolver resolver = HyperExpress.bind(Constants.Url.TABLE, status.getIndex().tableName())
                .bind(Constants.Url.DATABASE, status.getIndex().databaseName())
                .bind(Constants.Url.INDEX, status.getIndex().name())
                .bind(Constants.Url.INDEX_STATUS, status.getUuid().toString());

        // Include the Location header...
        String locationPattern = request.getNamedUrl(HttpMethod.GET, Constants.Routes.INDEX);
        response.addLocationHeader(LOCATION_BUILDER.build(locationPattern, resolver));
        // Return the newly-created resource...
        return status;
    }

    public Index read(Request request, Response response)
    {
        String database = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String table = request.getHeader(Constants.Url.TABLE, "No table provided");
        String name = request.getHeader(Constants.Url.INDEX, "No index name provided");
        Index entity = indexes.read(new Identifier(database, table, name));

        // enrich the entity with links, etc. here...
        HyperExpress.bind(Constants.Url.TABLE, entity.tableName())
                .bind(Constants.Url.DATABASE, entity.databaseName())
                .bind(Constants.Url.INDEX, entity.name());
//TODO: add: .bind(Constants.Url.INDEX_STATUS, status.getUuid().toString());
        return entity;
    }

    public List<Index> readAll(Request request, Response response)
    {
        String database = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String table = request.getHeader(Constants.Url.TABLE, "No table provided");

        HyperExpress.tokenBinder(new TokenBinder<Index>()
        {
            @Override
            public void bind(Index object, TokenResolver resolver)
            {
                resolver.bind(Constants.Url.TABLE, object.tableName())
                        .bind(Constants.Url.DATABASE, object.databaseName())
                        .bind(Constants.Url.INDEX, object.name());
                //TODO: add: .bind(Constants.Url.INDEX_STATUS, status.getUuid().toString());

            }
        });

        return indexes.readAll(database, table);
    }

//    public void update(Request request, Response response)
//    {
//        String database = request.getHeader(Constants.Url.DATABASE, "No database provided");
//        String table = request.getHeader(Constants.Url.TABLE, "No table provided");
//        String name = request.getHeader(Constants.Url.INDEX, "No index name provided");
//        Index entity = request.getBodyAs(Index.class, "Resource details not provided");
//        Table t = new Table();
//        t.database(database);
//        t.name(table);
//        entity.table(t);
//        entity.name(name);
//        indexes.update(entity);
//        response.setResponseNoContent();
//    }
    public void delete(Request request, Response response)
    {
        String database = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String table = request.getHeader(Constants.Url.TABLE, "No table provided");
        String name = request.getHeader(Constants.Url.INDEX, "No index name provided");
        indexes.delete(new Identifier(database, table, name));
        response.setResponseNoContent();
    }
}

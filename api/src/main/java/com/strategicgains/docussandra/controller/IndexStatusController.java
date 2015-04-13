package com.strategicgains.docussandra.controller;


import java.util.List;

import org.restexpress.Request;
import org.restexpress.Response;

import com.strategicgains.docussandra.Constants;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.IndexCreationStatus;
import com.strategicgains.docussandra.service.IndexService;
import com.strategicgains.hyperexpress.HyperExpress;
import com.strategicgains.hyperexpress.builder.TokenBinder;
import com.strategicgains.hyperexpress.builder.TokenResolver;
import com.strategicgains.hyperexpress.builder.UrlBuilder;
import java.util.UUID;
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
public class IndexStatusController
{

    private static final UrlBuilder LOCATION_BUILDER = new UrlBuilder();
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexStatusController.class);

    private IndexService indexes;

    public IndexStatusController(IndexService indexService)
    {
        super();
        this.indexes = indexService;
    }
   
    
    /**
     * Gets the status for an index creation request.
     * @param request
     * @param response
     * @return 
     */
    public IndexCreationStatus read(Request request, Response response){
        String id = request.getHeader(Constants.Url.INDEX_STATUS, "No index status id provided.");
        return indexes.status(UUID.fromString(id));
    }


    /**
     * Gets all presently active index creation requests.
     * @param request
     * @param response
     * @return 
     */
    public List<IndexCreationStatus> readAll(Request request, Response response)
    {
        return indexes.getAllActiveStatus();
    }

}

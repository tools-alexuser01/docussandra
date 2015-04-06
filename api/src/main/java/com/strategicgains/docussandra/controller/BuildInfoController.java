package com.strategicgains.docussandra.controller;

import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.commons.io.FileUtils;
import org.restexpress.ContentType;
import org.restexpress.Request;
import org.restexpress.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildInfoController
{

    private static final Logger LOGGER = LoggerFactory.getLogger(BuildInfoController.class);

    public Object getBuildInfo(Request request, Response response)
    {
        response.setResponseStatus(HttpResponseStatus.OK);
        response.setContentType(ContentType.JSON);
        try
        {
            return FileUtils.readFileToString(new File(this.getClass().getResource("./git.properties").toURI()));
        } catch (IOException | URISyntaxException e)
        {
            String message = "Could not read build info file.";
            LOGGER.error(message, e);
            return message;
        }
    }

}

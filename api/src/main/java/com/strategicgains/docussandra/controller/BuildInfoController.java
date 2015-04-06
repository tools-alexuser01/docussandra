package com.strategicgains.docussandra.controller;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.restexpress.ContentType;
import org.restexpress.Request;
import org.restexpress.Response;

public class BuildInfoController {

    public Object getBuildInfo(Request request, Response response) {
        response.setResponseStatus(HttpResponseStatus.OK);
        response.setContentType(ContentType.JSON);
        return "{\"isHealthy\":true}";
    }

}

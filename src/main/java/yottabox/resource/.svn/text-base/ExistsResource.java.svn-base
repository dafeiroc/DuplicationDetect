/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yottabox.resource;

import org.apache.hadoop.hbase.client.HBaseAdmin;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

public class ExistsResource extends ResourceBase {

    static CacheControl cacheControl;

    static {
        cacheControl = new CacheControl();
        cacheControl.setNoCache(true);
        cacheControl.setNoTransform(false);
    }

    String tableName;

    /**
     * Constructor
     *
     * @param table
     * @throws java.io.IOException
     */
    public ExistsResource(String table) throws IOException {
        super();
        this.tableName = table;
    }

    @GET
    @Produces({MIMETYPE_TEXT, MIMETYPE_XML, MIMETYPE_JSON,
            MIMETYPE_BINARY})
    public Response get(final @Context UriInfo uriInfo) {
        try {
            HBaseAdmin admin = new HBaseAdmin(servlet.getConfiguration());
            if (!admin.tableExists(tableName)) {
                throw new WebApplicationException(Response.Status.NOT_FOUND);
            }
        } catch (IOException e) {
            throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
        }
        /* if exists return true, if not response 404 or 503*/
        ResponseBuilder response = Response.ok(true, MIMETYPE_TEXT);
        response.cacheControl(cacheControl);
        return response.build();
    }
}

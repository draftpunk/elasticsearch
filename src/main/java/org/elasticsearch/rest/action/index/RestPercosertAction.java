/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.index;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.CREATED;
import static org.elasticsearch.rest.RestStatus.OK;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

/**
 *  The idea of percosert is to be able to take a document and instead of
 *  simply indexing it, we will percolate that document and store the 
 *  results of the percolate back in the original doc to be indexed.
 */
public class RestPercosertAction extends BaseRestHandler {

    @Inject
    public RestPercosertAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        
        controller.registerHandler(PUT, "/{index}/{type}/_percosert", new CreateHandler());
        controller.registerHandler(PUT, "/{index}/{type}/{id}/_percosert", new CreateHandler());
    }

    final class CreateHandler implements RestHandler {
        @Override
        public void handleRequest(RestRequest request, RestChannel channel) {
        	logger.info("RestIndexAction:CreateHandler");
            request.params().put("op_type", "percosert");
            RestPercosertAction.this.handleRequest(request, channel);
        }
    }
    
    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
    	
    	PercolateRequest percolateRequest = new PercolateRequest(request.param("index"), request.param("type"));
        percolateRequest.listenerThreaded(false);
        percolateRequest.source(request.content(), request.contentUnsafe());

        // we just send a response, no need to fork
        percolateRequest.listenerThreaded(false);
        // we don't spawn, then fork if local
        percolateRequest.operationThreaded(true);

        percolateRequest.preferLocal(request.paramAsBoolean("prefer_local", percolateRequest.preferLocalShard()));
        client.percolate(percolateRequest, new ActionListener<PercolateResponse>() {
            @Override
            public void onResponse(PercolateResponse percResponse) {
            	
                try {
                    for (String match : percResponse) {
                    	logger.info("match: " + match);
                    }
                    
                    IndexRequest indexRequest = new IndexRequest(request.param("index"), request.param("type"), request.param("id"));
                    indexRequest.listenerThreaded(false);
                    indexRequest.operationThreaded(true);
                    indexRequest.routing(request.param("routing"));
                    indexRequest.parent(request.param("parent")); // order is important, set it after routing, so it will set the routing
                    indexRequest.timestamp(request.param("timestamp"));
                    if (request.hasParam("ttl")) {
                        indexRequest.ttl(request.paramAsTime("ttl", null).millis());
                    }
                    indexRequest.source(request.content(), request.contentUnsafe());
                    
                    Map<String, Object> requestMap = indexRequest.sourceAsMap();
                    
                    // Remove query since we don't want to index that
                    if (requestMap.containsKey("query")) {
                    	requestMap.remove("query");
                    }                   
                    requestMap.put("percosert", percResponse);
                    indexRequest.source(requestMap);
                    
                                
                    indexRequest.timeout(request.paramAsTime("timeout", IndexRequest.DEFAULT_TIMEOUT));
                    indexRequest.refresh(request.paramAsBoolean("refresh", indexRequest.refresh()));
                    indexRequest.version(RestActions.parseVersion(request));
                    indexRequest.versionType(VersionType.fromString(request.param("version_type"), indexRequest.versionType()));
                    indexRequest.percolate(request.param("percolate", null));
                    String sOpType = request.param("op_type");
                    if (sOpType != null) {
                        if ("percosert".equals(sOpType)) {
                            indexRequest.opType(IndexRequest.OpType.INDEX);
                        } else {
                            try {
                                XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", "opType [" + sOpType + "] not allowed, either [index] or [create] are allowed").endObject()));
                            } catch (IOException e1) {
                                logger.warn("Failed to send response", e1);
                                return;
                            }
                        }
                    }
                    String replicationType = request.param("replication");
                    if (replicationType != null) {
                        indexRequest.replicationType(ReplicationType.fromString(replicationType));
                    }
                    String consistencyLevel = request.param("consistency");
                    if (consistencyLevel != null) {
                        indexRequest.consistencyLevel(WriteConsistencyLevel.fromString(consistencyLevel));
                    }
                    client.index(indexRequest, new ActionListener<IndexResponse>() {
                        @Override
                        public void onResponse(IndexResponse response) {
                            try {
                                XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                                builder.startObject()
                                        .field(Fields.OK, true)
                                        .field(Fields._INDEX, response.index())
                                        .field(Fields._TYPE, response.type())
                                        .field(Fields._ID, response.id())
                                        .field(Fields._VERSION, response.version());
                                if (response.matches() != null) {
                                    builder.startArray(Fields.MATCHES);
                                    for (String match : response.matches()) {
                                        builder.value(match);
                                    }
                                    builder.endArray();
                                }
                                builder.endObject();
                                RestStatus status = OK;
                                if (response.version() == 1) {
                                    status = CREATED;
                                }
                                channel.sendResponse(new XContentRestResponse(request, status, builder));
                            } catch (Exception e) {
                                onFailure(e);
                            }
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            try {
                                channel.sendResponse(new XContentThrowableRestResponse(request, e));
                            } catch (IOException e1) {
                                logger.error("Failed to send failure response", e1);
                            }
                        }
                    });

                    
                } catch (Exception e) {
                    onFailure(e);
                }               
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });

    	
    	

    }

    static final class Fields {
        static final XContentBuilderString OK = new XContentBuilderString("ok");
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        static final XContentBuilderString MATCHES = new XContentBuilderString("matches");
    }

}
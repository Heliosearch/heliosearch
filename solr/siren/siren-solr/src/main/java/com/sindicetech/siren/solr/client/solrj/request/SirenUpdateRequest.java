/**
 * Copyright (c) 2014, Sindice Limited. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sindicetech.siren.solr.client.solrj.request;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class SirenUpdateRequest extends SolrRequest {

  protected ModifiableSolrParams params;
  private final String jsonDocument;

  public static final String BOOST_PARAM = "boost";

  /**
   * Constructs a new request with a default uri of "/siren/add".
   */
  public SirenUpdateRequest(String jsonDocument) {
    super(SolrRequest.METHOD.POST, "/siren/add");
    this.jsonDocument = jsonDocument;
  }

  /**
   * Constructs a new request with the given request handler uri.
   *
   * @param uri The of the request handler.
   */
  public SirenUpdateRequest(String uri, String jsonDocument) {
    super(SolrRequest.METHOD.POST, uri);
    this.jsonDocument = jsonDocument;
  }

  @Override
  public Collection<ContentStream> getContentStreams() throws IOException {
    return ClientUtils.toContentStreams(jsonDocument, "application/json; charset=UTF-8");
  }

  /**
   * Set the document boost
   */
  public void setBoost(float boost) {
    this.setParam(BOOST_PARAM, Float.toString(boost));
  }

  public void setParam(String param, String value) {
    if (params == null) {
      params = new ModifiableSolrParams();
    }
    params.set(param, value);
  }

  /** Sets the parameters for this update request, overwriting any previous */
  public void setParams(ModifiableSolrParams params) {
    this.params = params;
  }

  @Override
  public ModifiableSolrParams getParams() {
    return params;
  }

  @Override
  public UpdateResponse process(SolrServer server) throws SolrServerException, IOException {
    long startTime = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
    UpdateResponse res = new UpdateResponse();
    res.setResponse(server.request(this));
    long endTime = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
    res.setElapsedTime(endTime - startTime);
    return res;
  }

}

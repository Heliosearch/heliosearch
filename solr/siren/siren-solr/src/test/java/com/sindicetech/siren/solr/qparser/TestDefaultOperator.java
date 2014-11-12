/**
 * Copyright (c) 2014, Sindice Limited. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
<<<<<<< HEAD:siren-solr/src/test/java/org/sindice/siren/solr/qparser/TestDefaultOperator.java
 * SIREn is not an open-source software. It is owned by Sindice Limited. SIREn
 * is licensed for evaluation purposes only under the terms and conditions of
 * the Sindice Limited Development License Agreement. Any form of modification
 * or reverse-engineering of SIREn is forbidden. SIREn is distributed without
 * any warranty.
=======
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
>>>>>>> develop:siren-solr/src/test/java/com/sindicetech/siren/solr/qparser/TestDefaultOperator.java
 */

package com.sindicetech.siren.solr.qparser;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.search.QueryParsing;
import org.junit.Test;

import com.sindicetech.siren.solr.BaseSolrServerTestCase;

import java.io.IOException;

public class TestDefaultOperator extends BaseSolrServerTestCase  {

  @Test
  public void testDefaultOperator()
  throws IOException, SolrServerException {
    this.addJsonString("1", "{ \"aaa\" : \"bbb ccc\" }");
    this.addJsonString("2", "{ \"aaa\" : \"bbb\" }");
    this.addJsonString("3", "{ \"aaa\" : \"ccc\" }");

    SolrQuery query = new SolrQuery();
    query.setQuery("bbb ccc");
    query.setRequestHandler("keyword");
    String[] results = this.search(query, URL_FIELD);
    // Default Operator = AND : Only one document should match
    assertEquals(1, results.length);

    query = new SolrQuery();
    query.setQuery("{ \"node\" : { \"query\" : \"bbb ccc\" } }");
    query.setRequestHandler("tree");
    results = this.search(query, URL_FIELD);
    // Default Operator = AND : Only one document should match
    assertEquals(1, results.length);
  }

  @Test
  public void testQOpParameter()
  throws IOException, SolrServerException {
    this.addJsonString("1", "{ \"aaa\" : \"bbb ccc\" }");
    this.addJsonString("2", "{ \"aaa\" : \"bbb\" }");
    this.addJsonString("3", "{ \"aaa\" : \"ccc\" }");

    SolrQuery query = new SolrQuery();
    query.setQuery("bbb ccc");
    query.setRequestHandler("keyword");
    query.set(QueryParsing.OP, "OR");
    String[] results = this.search(query, URL_FIELD);
    // Default Operator = OR : all the documents should match
    assertEquals(3, results.length);

    query = new SolrQuery();
    query.setQuery("{ \"node\" : { \"query\" : \"bbb ccc\" } }");
    query.setRequestHandler("tree");
    query.set(QueryParsing.OP, "OR");
    results = this.search(query, URL_FIELD);
    // Default Operator = OR : all the documents should match
    assertEquals(3, results.length);
  }

}

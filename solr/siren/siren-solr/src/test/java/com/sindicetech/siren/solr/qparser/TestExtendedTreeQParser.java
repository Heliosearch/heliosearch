/**
 * Copyright (c) 2014, Sindice Limited. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
<<<<<<< HEAD:siren-solr/src/test/java/org/sindice/siren/solr/qparser/TestExtendedTreeQParser.java
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
>>>>>>> develop:siren-solr/src/test/java/com/sindicetech/siren/solr/qparser/TestExtendedTreeQParser.java
 */
package com.sindicetech.siren.solr.qparser;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.junit.Test;

import com.sindicetech.siren.qparser.tree.dsl.QueryBuilder;
import com.sindicetech.siren.solr.BaseSolrServerTestCase;

import java.io.IOException;

public class TestExtendedTreeQParser extends BaseSolrServerTestCase {

  public void testNullJsonQuery()
  throws SolrServerException, IOException {
    final SolrQuery query = new SolrQuery();
    query.setRequestHandler("tree");
    this.search(query, ID_FIELD);
  }

  @Test(expected=SolrException.class)
  public void testEmptyJsonQuery()
  throws SolrServerException, IOException {
    final SolrQuery query = new SolrQuery();
    query.setQuery(" ");
    query.setRequestHandler("tree");
    this.search(query, ID_FIELD);
  }

  @Test(expected=SolrException.class)
  public void testBadJsonQuery()
  throws SolrServerException, IOException {
    final SolrQuery query = new SolrQuery();
    query.setQuery(" { aaa : } ");
    query.setRequestHandler("tree");
    this.search(query, ID_FIELD);
  }

  @Test
  public void testSimpleJsonQuery()
  throws IOException, SolrServerException, QueryNodeException {
    this.addJsonString("1", "{ \"aaa\" :  { \"bbb\" : \"ccc\" } }");

    SolrQuery query = new SolrQuery();
    final QueryBuilder b = new QueryBuilder();
    query.setQuery(b.newTwig("aaa").with(b.newNode("ccc"), 3).toString());
    query.setRequestHandler("tree");
    String[] results = this.search(query, ID_FIELD);
    assertEquals(1, results.length);

    query = new SolrQuery();
    query.setQuery(b.newTwig("aaa").with(b.newNode("ccc"), 2).toString());
    query.setRequestHandler("tree");
    results = this.search(query, ID_FIELD);
    assertEquals(0, results.length);
  }

  @Test
  public void testQNamesMapping()
  throws SolrServerException, IOException, QueryNodeException {
    this.addJsonString("1", "{ \"uri\" : { " +
    		"\"_value_\" : \"http://xmlns.com/foaf/0.1/Person\", " +
    		"\"_datatype_\" : \"uri\" " +
    		"} }");

    final SolrQuery query = new SolrQuery();
    final QueryBuilder b = new QueryBuilder();
    query.setQuery(b.newTwig("uri").with(b.newNode("'foaf:Person'")).toString());
    query.setRequestHandler("tree");
    final String[] results = this.search(query, ID_FIELD);
    assertEquals(1, results.length);
  }

  /**
   * Checks the qparser plugin option to allow leading wildcard. The solrconfig.xml is setting this parameter
   * to true.
   */
  @Test
  public void testAllowLeadingWildcard()
  throws SolrServerException, IOException, QueryNodeException {
    this.addJsonString("1", "{ \"aaa\" :  { \"bbb\" : \"ccc\" } }");

    final SolrQuery query = new SolrQuery();
    final QueryBuilder b = new QueryBuilder();
    query.setQuery(b.newNode("*a*").toString());
    query.setRequestHandler("tree");
    final String[] results = this.search(query, ID_FIELD);
    assertEquals(1, results.length);
  }

}

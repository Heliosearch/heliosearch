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
package com.sindicetech.siren.solr.qparser;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.Test;

import com.sindicetech.siren.qparser.tree.dsl.QueryBuilder;
import com.sindicetech.siren.solr.BaseSolrServerTestCase;

import java.io.IOException;

public class TestDatatypeQuery extends BaseSolrServerTestCase {

  @Test
  public void testTrieDatatypeQuery() throws IOException, SolrServerException {
    for (int i = 0; i < 1000; i++) {
      this.addJsonStringWoCommit("id"+i, "{ \"numeric\" : " + i + " }");
    }

    this.commit();

    final SolrQuery query = new SolrQuery();
    // by default, the json tokenizer index numeric value as long
    query.setQuery("numeric : xsd:long([501 TO *])");
    query.setRequestHandler("keyword");
    assertEquals(499, this.search(query).getNumFound());
  }

  @Test
  public void testTrieDateDatatypeQuery() throws IOException, SolrServerException {
    for (int i = 0; i < 1000; i++) {
      this.addJsonStringWoCommit("id"+i, "{ \"date\" : { " +
      		        "\"_value_\" : \"" + (1000 + i) + "-12-31T00:00:00Z\", " +
      		    		"\"_datatype_\" : \"http://www.w3.org/2001/XMLSchema#date\"" +
      		"} }");
    }

    this.commit();

    final SolrQuery query = new SolrQuery();
    // by default, the json tokenizer index numeric value as long
    query.setQuery("date : xsd:date([1995-12-31T00:00:00Z TO 1995-12-31T00:00:00Z+5YEARS])");
    query.setRequestHandler("keyword");
    assertEquals(5, this.search(query).getNumFound());
  }

  @Test
  public void testURIDatatypeQuery()
  throws SolrServerException, IOException, QueryNodeException {
    this.addJsonString("1", "{ \"uri\" : { " +
        "\"_value_\" : \"http://xmlns.com/foaf/0.1/Person\", " +
        "\"_datatype_\" : \"uri\" " +
        "} }");

    final SolrQuery query = new SolrQuery();
    final QueryBuilder b = new QueryBuilder();
    query.setQuery(b.newTwig("uri").with(b.newNode("uri('http://xmlns.com/foaf/0.1/Person')")).toString());
    query.setRequestHandler("tree");
    final String[] results = this.search(query, ID_FIELD);
    assertEquals(1, results.length);
  }

  @Test
  public void testBooleanDatatypeQuery()
  throws SolrServerException, IOException, QueryNodeException {
    this.addJsonString("1", "{ \"boolean\" : true }");

    final SolrQuery query = new SolrQuery();
    query.setQuery("true");
    query.setRequestHandler("keyword");
    final String[] results = this.search(query, ID_FIELD);
    assertEquals(1, results.length);
  }

  @Test
  public void testDoubleQuery()
  throws SolrServerException, IOException, QueryNodeException {
    this.addJsonString("1", "{\"@id\":\"http://therealbanff.com/event/112\",\"costValue\":{\"_datatype_\":\"http://www.w3.org/2001/XMLSchema#double\",\"_value_\":\"0.00\"}}");

    final SolrQuery query = new SolrQuery();
    final QueryBuilder b = new QueryBuilder();
    query.setQuery(b.newNode("xsd:double([-1.0 TO 1.0])").toString());
    query.setRequestHandler("tree");
    final String[] results = this.search(query, ID_FIELD);
    assertEquals(1, results.length);
  }

}

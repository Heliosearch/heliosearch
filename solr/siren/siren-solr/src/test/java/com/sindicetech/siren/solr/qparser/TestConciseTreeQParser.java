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
import org.junit.BeforeClass;
import org.junit.Test;

import com.sindicetech.siren.qparser.tree.dsl.ConciseQueryBuilder;
import com.sindicetech.siren.qparser.tree.dsl.TwigQuery;
import com.sindicetech.siren.solr.SolrServerTestCase;

import java.io.IOException;

public class TestConciseTreeQParser extends SolrServerTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-concise.xml", "schema-concise.xml", SOLR_HOME);
  }

  @Test
  public void testSimpleConciseTreeQuery()
  throws IOException, SolrServerException, QueryNodeException {
    this.addJsonString("1", "concise", "{ \"aaa\" :  { \"bbb\" : \"ccc\" } }");

    SolrQuery query = new SolrQuery();
    final ConciseQueryBuilder b = new ConciseQueryBuilder();
    query.setQuery(b.newTwig("aaa").with(b.newNode("ccc").setAttribute("bbb")).toString());
    query.setRequestHandler("tree");
    query.set("qf", "concise");
    String[] results = this.search(query, ID_FIELD);
    assertEquals(1, results.length);
  }

  /**
   * Checks that a keyword query that matches no document (the term down is in the stop list)
   * does not throw exception and returns an empty result set.
   *
   * See #73.
   */
  @Test
  public void testKeywordQueryMatchingNoDoc()
  throws IOException, SolrServerException, QueryNodeException {
    this.addJsonString("1", "concise", "{ \"aaa\" :  { \"bbb\" : \"ccc\" } }");

    SolrQuery query = new SolrQuery();
    query.setQuery("{\"node\":{\"query\":\"down\"}}");
    query.setRequestHandler("tree");
    query.set("qf", "concise");
    String[] results = this.search(query, ID_FIELD);
    assertEquals(0, results.length);
  }

  @Test
  public void testConciseTreeAttributeWildcard()
  throws IOException, SolrServerException, QueryNodeException {
    this.addJsonString("1", "concise-attribute-wildcard", "{ \"aaa\" :  { \"bbb\" : \"ccc\" } }");

    SolrQuery query = new SolrQuery();
    final ConciseQueryBuilder b = new ConciseQueryBuilder();
    query.setQuery(b.newNode("ccc").toString());
    query.setRequestHandler("tree");
    query.set("qf", "concise-attribute-wildcard");
    String[] results = this.search(query, ID_FIELD);
    assertEquals(1, results.length);
  }

  @Test
  public void testSpanConciseTreeQuery()
  throws IOException, SolrServerException, QueryNodeException {
    this.addJsonString("1", "concise", "{ \"aaa\" :  { \"bbb\" : \"ccc\", \"ddd\" : \"eee\" } }");

    SolrQuery query = new SolrQuery();
    final ConciseQueryBuilder b = new ConciseQueryBuilder();

    final TwigQuery twig = b.newTwig("aaa")
                            .with(b.newBoolean()
                                   .with(b.newNode("ccc").setAttribute("bbb"))
                                   .with(b.newNode("eee").setAttribute("ddd"))
                                   .setInOrder(false));

    query.setQuery(twig.toString());
    query.setRequestHandler("tree");
    query.set("qf", "concise");
    String[] results = this.search(query, ID_FIELD);
    assertEquals(1, results.length);
  }

  /**
   * Issue #66: The query parser applies lowercase the expanded terms
   */
  @Test
  public void testPrefixQueryConciseTreeQuery()
  throws IOException, SolrServerException, QueryNodeException {
    this.addJsonString("1", "concise-keyword", "{ \"rdfs:label\" :  \"COM_GRANAGLIONE\" }");

    SolrQuery query = new SolrQuery();
    final ConciseQueryBuilder b = new ConciseQueryBuilder();
    query.setQuery(b.newNode("COM_*").setAttribute("rdfs:label").toString());
    query.setRequestHandler("tree");
    query.set("qf", "concise-keyword");
    String[] results = this.search(query, ID_FIELD);
    assertEquals(1, results.length);
  }

  @Test
  public void testDebugConciseTreeQuery()
  throws IOException, SolrServerException, QueryNodeException {
    this.addJsonString("1", "concise", "{\"@graph\":[{\"@id\":\"http://dbpedia/Person/1\",\"http://www.w3.org/2000/01/rdf-schema#label\":\"Alfred Hitchkoc\",\"http://www.w3.org/2000/01/rdf-schema#seeAlso\":{\"@id\":\"http://dbpedia/Person/2\",\"http://www.w3.org/2000/01/rdf-schema#label\":\"Brad Pitt\"}},{\"@id\":\"http://dbpedia/Person/2\",\"http://www.w3.org/2000/01/rdf-schema#label\":\"Brad Pitt\"}]}");
    this.addJsonString("2", "concise", "{\"@graph\":[{\"http://www.w3.org/2000/01/rdf-schema#label\":\"Alfred Hitchkoc\",\"@id\":\"http://dbpedia/Person/1\",\"http://www.w3.org/2000/01/rdf-schema#seeAlso\":{\"@id\":\"http://dbpedia/Person/2\",\"http://www.w3.org/2000/01/rdf-schema#label\":\"Brad Pitt\"}},{\"@id\":\"http://dbpedia/Person/2\",\"http://www.w3.org/2000/01/rdf-schema#label\":\"Brad Pitt\"}]}");

    SolrQuery query = new SolrQuery();
    final ConciseQueryBuilder b = new ConciseQueryBuilder();

//    String q = b.newTwig()
//                .with(
//      b.newTwig("http://www.w3.org/2000/01/rdf-schema#seeAlso")
//       .with(b.newNode("'http://dbpedia/Person/2'").setAttribute("@id"))
//                )
//                .with(
//      b.newNode("Alfred Hitchkoc").setAttribute("http://www.w3.org/2000/01/rdf-schema#label")
//                ).toString();

    String q = b.newTwig()
    .with(
    b.newTwig("http://www.w3.org/2000/01/rdf-schema#seeAlso")
    .with(b.newNode("'http://dbpedia/Person/2'").setAttribute("@id")), 1
    )
    .with(
    b.newNode("Alfred Hitchkoc").setAttribute("http://www.w3.org/2000/01/rdf-schema#label"), 1
    ).toString();

    query.setQuery(q);
    query.setRequestHandler("tree");
    query.set("qf", "concise");
    String[] results = this.search(query, ID_FIELD);
    assertEquals(2, results.length);
  }

}

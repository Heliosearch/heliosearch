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

import com.sindicetech.siren.solr.BaseSolrServerTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.Test;

import java.io.IOException;

public class TestURIQuery extends BaseSolrServerTestCase {

  @Test
  public void testTildeInURI() throws IOException, SolrServerException {
    this.addJsonString("1", " { \"uri\" : { " +
    		"\"_value_\" : \"http://sw.deri.org/~aidanh/\", " +
    		"\"_datatype_\" : \"uri\"" +
    		" } }");

    SolrQuery query = new SolrQuery();
    query.setQuery("uri('http://sw.deri.org/~aidanh/')");
    query.setRequestHandler("keyword");
    assertEquals(1, this.search(query).getNumFound());

    // test uri trailing slash filter
    query = new SolrQuery();
    query.setQuery("uri('http://sw.deri.org/~aidanh/')");
    query.setRequestHandler("keyword");
    assertEquals(1, this.search(query).getNumFound());
  }

  @Test
  public void testEncodedURI() throws IOException, SolrServerException {
    this.addJsonString("1", " { \"uri\" : { " +
        "\"_value_\" : \"http://dblp.l3s.de/d2r/resource/authors/Knud_M%C3%B6ller\", " +
        "\"_datatype_\" : \"uri\"" +
        " } }");

    // testing search of plain URI search
    SolrQuery query = new SolrQuery();
    query.setQuery("uri('http://dblp.l3s.de/d2r/resource/authors/Knud_M%C3%B6ller')");
    query.setRequestHandler("keyword");
    assertEquals(1, this.search(query).getNumFound());

    // testing search of decoded local name token
    query = new SolrQuery();
    query.setQuery("Möller");
    query.setRequestHandler("keyword");
    assertEquals(1, this.search(query).getNumFound());
  }

}

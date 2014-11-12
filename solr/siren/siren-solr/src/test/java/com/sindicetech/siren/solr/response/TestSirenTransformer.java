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
package com.sindicetech.siren.solr.response;

import com.sindicetech.siren.solr.SolrServerTestCase;
import com.sindicetech.siren.solr.qparser.SirenParams;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestSirenTransformer extends SolrServerTestCase {
  private static final String json =
      "{"
        + "\"title\": \"Unforgiven\","
        + "\"year\": \"1992\","
        + "\"genre\": \"Western\","
        + "\"summary\": \"The town of Big Whisky is full of normal people villainy, man and myth.\","
        + "\"country\": \"USA\","

      + "\"director\": {"
          + "\"last_name\": \"Eastwood\","
          + "\"first_name\": \"Clint\","
          + "\"birth_date\": \"1930\""
      + "},"
      + "\"actors\": ["
        + "{"
            + "\"first_name\": \"Clint\","
            + "\"last_name\": \"Eastwood\","
            + "\"birth_date\": \"1930\","
            + "\"role\": \"William Munny\""
        + "},"
        + "{"
            + "\"first_name\": \"Gene\","
            + "\"last_name\": \"Hackman\","
            + "\"birth_date\": \"1930\","
            + "\"role\": \"Little Bill Dagget\""
        + "},"
        + "{"
            + "\"first_name\": \"Morgan\","
            + "\"last_name\": \"Freeman\","
            + "\"birth_date\": \"1937\","
            + "\"role\": \"Ned Logan\""
        + "}"
      + "]"
    + "}";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig_with_transformer.xml", "schema-sirenfield-stored.xml", SOLR_HOME);
  }

  @Test
  public void testTransformer() throws SolrServerException, IOException {
    SolrInputDocument document = new SolrInputDocument();
    document.addField(ID_FIELD, "1");
    document.addField(JSON_FIELD, json);
    getWrapper().add(document);
    getWrapper().commit();

    String queryStr = "{"
        + "\"twig\" : { "
          + "\"root\" : \"director\","
           + "\"child\" : [{\"occur\":\"MUST\","
                      + "\"twig\": {"
                           + "\"child\":[{\"occur\":\"MUST\","
                              + "\"twig\": {"
                                   + "\"root\":\"first_name\","
                                  + "\"child\":["
                                        + "{\"occur\":\"MUST\","
                                              + "\"node\" : { \"query\" : \"Clint\" } "
                                         + "}"
                                          + "]"
                                       + "}"
                                     + "}"
                                    + "]"
                               + "}"
                       + "}"
                       + ", {\"occur\" : \"MUST\", \"variable\" : {}}"
                      + "]"
                  + "}"
        + "}";

    final SolrQuery query = new SolrQuery();
    query.setQuery(queryStr);
    query.setRequestHandler("tree");
    query.set(SirenParams.QF, JSON_FIELD);
    query.set("fl", "id,json,[sirenProjection]");
    final String[] results = getWrapper().search(query, JSON_FIELD);
    assertEquals(1, results.length);
    assertEquals("{\"director\":{\"last_name\":\"Eastwood\",\"first_name\":\"Clint\",\"birth_date\":\"1930\"}}", results[0]);
  }

}

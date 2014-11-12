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
package com.sindicetech.siren.solr.schema;

import com.sindicetech.siren.solr.SolrServerTestCase;
import com.sindicetech.siren.solr.analysis.*;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestConciseJsonField extends SolrServerTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-concise.xml", "schema-concise.xml", SOLR_HOME);
  }

  @Test
  public void testConciseSirenFieldType() throws Exception {
    final IndexSchema schema = h.getCore().getLatestSchema();
    SchemaField json = schema.getField("concise");
    assertNotNull(json);
    FieldType tmp = json.getType();
    assertTrue(tmp instanceof ConciseJsonField);

    json = schema.getField("concise-attribute-wildcard");
    assertNotNull(json);
    tmp = json.getType();
    assertTrue(tmp instanceof ConciseJsonField);
  }

  @Test
  public void testConciseSirenFieldAnalyzer() throws Exception {
    final IndexSchema schema = h.getCore().getLatestSchema();
    final SchemaField json = schema.getField("concise");
    final FieldType tmp = json.getType();

    assertTrue(tmp.getAnalyzer() instanceof TokenizerChain);
    final TokenizerChain ts = (TokenizerChain) tmp.getAnalyzer();
    assertNotNull(ts.getTokenizerFactory());
    assertTrue(ts.getTokenizerFactory() instanceof ConciseJsonTokenizerFactory);

    // 4 filters for index analyzer
    assertNotNull(ts.getTokenFilterFactories());
    assertEquals(4, ts.getTokenFilterFactories().length);
    assertTrue(ts.getTokenFilterFactories()[0] instanceof DatatypeAnalyzerFilterFactory);
    assertTrue(ts.getTokenFilterFactories()[1] instanceof PathEncodingFilterFactory);
    assertTrue(ts.getTokenFilterFactories()[2] instanceof PositionAttributeFilterFactory);
    assertTrue(ts.getTokenFilterFactories()[3] instanceof SirenPayloadFilterFactory);
  }

}

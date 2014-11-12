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

import com.sindicetech.siren.analysis.IntNumericAnalyzer;
import com.sindicetech.siren.index.codecs.siren10.Siren10AForPostingsFormat;
import com.sindicetech.siren.solr.SolrServerTestCase;
import com.sindicetech.siren.solr.analysis.DatatypeAnalyzerFilterFactory;
import com.sindicetech.siren.solr.analysis.ExtendedJsonTokenizerFactory;
import com.sindicetech.siren.solr.analysis.PositionAttributeFilterFactory;
import com.sindicetech.siren.solr.analysis.SirenPayloadFilterFactory;
import org.apache.lucene.analysis.core.WhitespaceTokenizerFactory;
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizerFactory;
import org.apache.lucene.document.FieldType.NumericType;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestExtendedJsonField extends SolrServerTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema-sirenfield.xml", SOLR_HOME);
  }

  @Test
  public void testSirenFieldType() throws Exception {
    final IndexSchema schema = h.getCore().getLatestSchema();
    final SchemaField ntriple = schema.getField(JSON_FIELD);
    assertNotNull(ntriple);
    final FieldType tmp = ntriple.getType();
    assertTrue(tmp instanceof ExtendedJsonField);
  }

  @Test
  public void testSirenFieldTypeProperties() throws Exception {
    final IndexSchema schema = h.getCore().getLatestSchema();
    final FieldType type = schema.getField("json").getType();
    assertTrue(type instanceof ExtendedJsonField);
    assertFalse(type.isMultiValued());
    assertTrue(type.isTokenized());
    assertEquals(type.getPostingsFormat(), Siren10AForPostingsFormat.NAME);
  }

  @Test(expected=SolrException.class)
  public void testSirenFieldInstancePropertyMultiValued()
  throws IOException, SolrServerException {
    this.addJsonString("1", "siren-multiValued", "{ \"a\" : \"b\" }");
  }

  @Test(expected=SolrException.class)
  public void testSirenFieldInstancePropertyOmitTermFreqAndPositions()
  throws IOException, SolrServerException {
    this.addJsonString("1", "siren-omitTermFreqAndPositions", "{ \"a\" : \"b\" }");
  }

  @Test(expected=SolrException.class)
  public void testSirenFieldInstancePropertyTermVectors()
  throws IOException, SolrServerException {
    this.addJsonString("1", "siren-termVectors", "{ \"a\" : \"b\" }");
  }

  @Test
  public void testSirenFieldAnalyzer() throws Exception {
    final IndexSchema schema = h.getCore().getLatestSchema();
    final SchemaField ntriple = schema.getField(JSON_FIELD);
    final FieldType tmp = ntriple.getType();

    assertTrue(tmp.getAnalyzer() instanceof TokenizerChain);
    final TokenizerChain ts = (TokenizerChain) tmp.getAnalyzer();
    assertNotNull(ts.getTokenizerFactory());
    assertTrue(ts.getTokenizerFactory() instanceof ExtendedJsonTokenizerFactory);

    // 3 filters for index analyzer
    assertNotNull(ts.getTokenFilterFactories());
    assertEquals(3, ts.getTokenFilterFactories().length);
    assertTrue(ts.getTokenFilterFactories()[0] instanceof DatatypeAnalyzerFilterFactory);
    assertTrue(ts.getTokenFilterFactories()[1] instanceof PositionAttributeFilterFactory);
    assertTrue(ts.getTokenFilterFactories()[2] instanceof SirenPayloadFilterFactory);
  }

  @Test
  public void testSirenFieldDatatypeAnalyzer() throws Exception {
    final IndexSchema schema = h.getCore().getLatestSchema();
    final SchemaField ntriple = schema.getField(JSON_FIELD);
    final FieldType tmp = ntriple.getType();

    TokenizerChain ts = (TokenizerChain) tmp.getAnalyzer();

    assertTrue(ts.getTokenFilterFactories()[0] instanceof DatatypeAnalyzerFilterFactory);
    final DatatypeAnalyzerFilterFactory f = (DatatypeAnalyzerFilterFactory) ts.getTokenFilterFactories()[0];
    assertNotNull(f.getDatatypeAnalyzers());
    assertEquals(9, f.getDatatypeAnalyzers().size());

    assertNotNull(f.getDatatypeAnalyzers().get("http://json.org/field"));
    ts = (TokenizerChain) f.getDatatypeAnalyzers().get("http://json.org/field");
    assertNotNull(ts.getTokenizerFactory());
    assertTrue(ts.getTokenizerFactory() instanceof WhitespaceTokenizerFactory);

    assertNotNull(f.getDatatypeAnalyzers().get("http://www.w3.org/2001/XMLSchema#string"));
    ts = (TokenizerChain) f.getDatatypeAnalyzers().get("http://www.w3.org/2001/XMLSchema#string");
    assertNotNull(ts.getTokenizerFactory());
    assertTrue(ts.getTokenizerFactory() instanceof UAX29URLEmailTokenizerFactory);

    assertNotNull(f.getDatatypeAnalyzers().get("http://www.w3.org/2001/XMLSchema#int"));
    assertTrue(f.getDatatypeAnalyzers().get("http://www.w3.org/2001/XMLSchema#int") instanceof IntNumericAnalyzer);
    final IntNumericAnalyzer a = (IntNumericAnalyzer) f.getDatatypeAnalyzers().get("http://www.w3.org/2001/XMLSchema#int");
    assertEquals(8, a.getPrecisionStep());
    assertEquals(32, a.getNumericParser().getValueSize());
    assertEquals(NumericType.INT, a.getNumericParser().getNumericType());
  }

  @Test
  public void testLukeRequest() throws IOException, SolrServerException {
    LukeRequest req = new LukeRequest();
    req.setShowSchema(true);
    final LukeResponse response = req.process(this.getWrapper().getServer());
    assertTrue(!response.getFieldInfo().isEmpty());
  }

}

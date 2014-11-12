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
package com.sindicetech.siren.solr.handler;

import com.sindicetech.siren.qparser.tree.dsl.ConciseQueryBuilder;
import com.sindicetech.siren.solr.SolrServerTestCase;
import com.sindicetech.siren.solr.client.solrj.request.SirenUpdateRequest;
import com.sindicetech.siren.solr.handler.mapper.IdFieldMapper;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.servlet.DirectSolrConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class TestSirenUpdateRequestHandler extends SolrServerTestCase {

  private static final String HANDLER_NAME = "/siren/add";

  private static final String SOLRCONFIG_XML = "solrconfig-update.xml";
  private static final String SCHEMA_XML     = "schema-update.xml";
  private static final String QNAMES_TXT     = "qnames.txt";
  private static final String DATATYPES_XML  = "datatypes.xml";
  private static final String STOPWORDS_TXT  = "stopwords.txt";

  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";

  @Before
  public void initManagedSchemaCore() throws Exception {
    File tmpSolrHome = createTempDir();
    File tmpConfDir = new File(tmpSolrHome, confDir);
    File testHomeConfDir = new File(SOLR_HOME, confDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, SOLRCONFIG_XML), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, SCHEMA_XML), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, QNAMES_TXT), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, DATATYPES_XML), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, STOPWORDS_TXT), tmpConfDir);

    // initCore will trigger an upgrade to managed schema, since the solrconfig*.xml has
    // <schemaFactory class="ManagedIndexSchemaFactory" ... />
    initCore(SOLRCONFIG_XML, SCHEMA_XML, tmpSolrHome.getPath());
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    deleteCore();
  }

  private String sendUpdateRequest(String input, SolrParams params) {
    try (SolrCore core = h.getCoreInc()) {
      DirectSolrConnection connection = new DirectSolrConnection(core);
      SolrRequestHandler handler = core.getRequestHandler(HANDLER_NAME);
      return connection.request(handler, params, input);
    }
    catch (SolrException e) {
      throw e;
    }
    catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }

  private String sendUpdateRequest(String input) {
    return this.sendUpdateRequest(input, null);
  }

  /**
   * Check that a UUID is generated for JSON documents with no 'id' attribute.
   */
  @Test
  public void testJsonWithNoId() throws IOException, SolrServerException {
    String input = "{ \"aaa\" :  \"bbb\" }";
    this.sendUpdateRequest(input);
    this.commit();

    SolrQuery query = new SolrQuery();
    query.setParam("nested", "{!lucene} *:*");
    query.setRequestHandler("tree");
    query.setFields(IdFieldMapper.INPUT_FIELD);

    SolrDocumentList results = this.search(query);
    assertEquals(1, results.getNumFound());
    assertNotNull(results.get(0).getFieldValue(IdFieldMapper.INPUT_FIELD));
  }

  /**
   * Check that the value type of the id field do not have impact on the indexing.
   */
  @Test
  public void testJsonWithNumericId() throws IOException, SolrServerException {
    String input1 = "{ \"id\" : 1, \"aaa\" : null }";
    String input2 = "{ \"id\" : \"2\", \"aaa\" : null }";

    this.sendUpdateRequest(input1);
    this.sendUpdateRequest(input2);
    this.commit();

    SolrQuery query = new SolrQuery();
    query.setParam("nested", "{!lucene} id:1");
    query.setRequestHandler("tree");
    long found = this.search(query).getNumFound();
    assertEquals(1, found);

    query = new SolrQuery();
    query.setParam("nested", "{!lucene} id:2");
    query.setRequestHandler("tree");
    found = this.search(query).getNumFound();
    assertEquals(1, found);
  }

  /**
   * Check that the JSON document is correctly indexed in a SIREn's json field.
   */
  @Test
  public void testJsonField() throws QueryNodeException, IOException, SolrServerException {
    String input = "{ \"id\" : \"1\", \"aaa\" :  \"bbb\" }";

    this.sendUpdateRequest(input);
    this.commit();

    SolrQuery query = new SolrQuery();
    final ConciseQueryBuilder b = new ConciseQueryBuilder();
    query.setQuery(b.newNode("bbb").setAttribute("aaa").toString());
    query.setRequestHandler("tree");
    long found = this.search(query).getNumFound();
    assertEquals(1, found);
  }

  /**
   * Check if the field is stored as indicated by the fieldtype of the associated path-based mapper.
   */
  @Test
  public void testStoredField() throws QueryNodeException, IOException, SolrServerException {
    String input = "{ \"id\" : \"1\", \"aaa\" :  \"bbb\" }";

    this.sendUpdateRequest(input);
    this.commit();

    SolrQuery query = new SolrQuery();
    final ConciseQueryBuilder b = new ConciseQueryBuilder();
    query.setQuery(b.newNode("bbb").setAttribute("aaa").toString());
    query.setRequestHandler("tree");
    query.setFields("aaa");

    SolrDocumentList result = this.search(query);
    assertEquals(1, result.getNumFound());
    assertNotNull(result.get(0).getFieldValue("aaa"));
    assertTrue(result.get(0).getFieldValue("aaa") instanceof ArrayList);
    assertEquals("bbb", ((ArrayList) result.get(0).getFieldValue("aaa")).get(0));
  }

  /**
   * Test the required mappers. The fields 'aaa' is required but not existing in the document. It should throw
   * an exception.
   */
  @Test(expected=SolrException.class)
  public void testRequiredMapper() throws QueryNodeException, IOException, SolrServerException {
    String input = "{ \"id\" : \"1\" }";

    this.sendUpdateRequest(input);
  }

  /**
   * Test the optional mappers. The fields 'bbb' and 'ddd' are not defined, but the String and Boolean
   * value types are defined. They should be indexed.
   */
  @Test
  public void testOptionalMapper() throws QueryNodeException, IOException, SolrServerException {
    String input = "{ \"id\" : \"1\", \"aaa\" : null, \"bbb\" :  \"ccc\", \"ddd\" : false }";

    this.sendUpdateRequest(input);
    this.commit();

    SolrQuery query = new SolrQuery();
    query.setParam("nested", "{!lucene} bbb:ccc");
    query.setRequestHandler("tree");
    long found = this.search(query).getNumFound();
    assertEquals(1, found);

    query = new SolrQuery();
    query.setParam("nested", "{!lucene} ddd:false");
    query.setRequestHandler("tree");
    found = this.search(query).getNumFound();
    assertEquals(1, found);
  }

  /**
   * Test the default mapper and the null field type. The field 'bbb' with an Integer value should
   * not match any mapper, and therefore be processed by the default mapper. The default mapper
   * is configured with the null field type which indicates to ignore the field. Therefore the
   * field 'bbb' should not be indexed and the search should throw an exception.
   */
  @Test(expected=SolrException.class)
  public void testDefaultNullMapper() throws QueryNodeException, IOException, SolrServerException {
    String input = "{ \"id\" : \"1\", \"bbb\" :  1 }";

    this.sendUpdateRequest(input);
    this.commit();

    SolrQuery query = new SolrQuery();
    query.setParam("nested", "{!lucene} bbb:1");
    query.setRequestHandler("tree");
    this.search(query);
  }

  /**
   * Test the null field type. The path aaa.bbb is configured with the null field type which indicates to ignore the
   * field. Therefore the field 'aaa.bbb' should not be indexed and the search should throw an exception.
   */
  @Test(expected=SolrException.class)
  public void testNullMapper() throws QueryNodeException, IOException, SolrServerException {
    String input = "{ \"id\" : \"1\", \"aaa\" : { \"bbb\" : \"ccc\" } }";

    this.sendUpdateRequest(input);
    this.commit();

    SolrQuery query = new SolrQuery();
    query.setParam("nested", "{!lucene} aaa.bbb:ccc");
    query.setRequestHandler("tree");
    this.search(query);
  }

  /**
   * Test mixed array. The String and Double type is defined, while the Integer is undefined. Integer will be ignored
   * and not indexed. The Double value will be indexed as a string.
   */
  @Test
  public void testMixedTypeArray() throws QueryNodeException, IOException, SolrServerException {
    String input = "{ \"id\" : \"1\", \"aaa\" : null, \"bbb\" :  [\"ccc\", 2, 3.1] }";

    this.sendUpdateRequest(input);
    this.commit();

    SolrQuery query = new SolrQuery();
    query.setParam("nested", "{!lucene} bbb:ccc");
    query.setRequestHandler("tree");
    long found = this.search(query).getNumFound();
    assertEquals(1, found);

    query = new SolrQuery();
    query.setParam("nested", "{!lucene} bbb:2");
    query.setRequestHandler("tree");
    found = this.search(query).getNumFound();
    assertEquals(0, found);

    query = new SolrQuery();
    query.setParam("nested", "{!lucene} bbb:3.1");
    query.setRequestHandler("tree");
    found = this.search(query).getNumFound();
    assertEquals(1, found);
  }

  /**
   * Check that nested array are properly flattened.
   */
  @Test
  public void testNestedArray() throws QueryNodeException, IOException, SolrServerException {
    String input = "{ \"id\" : \"1\", \"aaa\" : null, \"bbb\" :  [\"ccc\", [ \"ddd\", [\"eee\"] ] ] }";

    this.sendUpdateRequest(input);
    this.commit();

    SolrQuery query = new SolrQuery();
    query.setParam("nested", "{!lucene} bbb:ccc");
    query.setRequestHandler("tree");
    long found = this.search(query).getNumFound();
    assertEquals(1, found);

    query = new SolrQuery();
    query.setParam("nested", "{!lucene} bbb:ddd");
    query.setRequestHandler("tree");
    found = this.search(query).getNumFound();
    assertEquals(1, found);
  }

  /**
   * Check that nested objects are properly flattened.
   */
  @Test
  public void testNestedObject() throws QueryNodeException, IOException, SolrServerException {
    String input = "{ \"id\" : \"1\", \"aaa\" : null, \"bbb\" :  { \"ccc\" : { \"ddd\" : \"eee\" }, \"fff\" : \"ggg\" } }";

    this.sendUpdateRequest(input);
    this.commit();

    SolrQuery query = new SolrQuery();
    query.setParam("nested", "{!lucene} bbb.ccc.ddd:eee");
    query.setRequestHandler("tree");
    long found = this.search(query).getNumFound();
    assertEquals(1, found);

    query = new SolrQuery();
    query.setParam("nested", "{!lucene} bbb.fff:ggg");
    query.setRequestHandler("tree");
    found = this.search(query).getNumFound();
    assertEquals(1, found);
  }

  /**
   * Check that nested objects in a mixed array are properly flattened.
   */
  @Test
  public void testNestedObjectInArray() throws QueryNodeException, IOException, SolrServerException {
    String input = "{ \"id\" : \"1\", \"aaa\" : null, \"bbb\" :  [ \"ccc\", { \"ddd\" : \"eee\" }, \"ggg\" ] }";

    this.sendUpdateRequest(input);
    this.commit();

    SolrQuery query = new SolrQuery();
    query.setParam("nested", "{!lucene} bbb:ccc");
    query.setRequestHandler("tree");
    long found = this.search(query).getNumFound();
    assertEquals(1, found);

    query = new SolrQuery();
    query.setParam("nested", "{!lucene} bbb.ddd:eee");
    query.setRequestHandler("tree");
    found = this.search(query).getNumFound();
    assertEquals(1, found);
  }

  /**
   * Check that the special SIREn's object for datatype is properly flattened.
   */
  @Test
  public void testDatatypeObject() throws QueryNodeException, IOException, SolrServerException {
    String input = "{ \"id\" : \"1\", \"aaa\" : null, \"bbb\" :  { \"_datatype_\" : \"uri\", \"_value_\" : \"ccc\" } }";

    this.sendUpdateRequest(input);
    this.commit();

    SolrQuery query = new SolrQuery();
    query.setParam("nested", "{!lucene} bbb:ccc");
    query.setRequestHandler("tree");
    long found = this.search(query).getNumFound();
    assertEquals(1, found);
  }

  /**
   * Check that the nested query and the main query are intersected, i.e., that each one is assigned a MUST operator.
   * See issue #60.
   */
  @Test
  public void testNestedQuery() throws IOException, SolrServerException, QueryNodeException {
    String input = "{ \"aaa\" : null, \"ChargeDeviceRef\" : \"CM765\", \"ChargeDeviceLocation\" : { \"Address\" : { \"PostTown\" : \"Peterborough\" } } }";
    this.sendUpdateRequest(input);
    input = "{ \"aaa\" : null, \"ChargeDeviceRef\" : \"CM556\", \"ChargeDeviceLocation\" : { \"Address\" : { \"PostTown\" : \"Peterborough\" } } }";
    this.sendUpdateRequest(input);
    input = "{ \"aaa\" : null, \"ChargeDeviceRef\" : \"CM779\", \"ChargeDeviceLocation\" : { \"Address\" : { \"PostTown\" : \"Peterborough\" } } }";
    this.sendUpdateRequest(input);

    this.commit();

    SolrQuery query = new SolrQuery();
    final ConciseQueryBuilder b = new ConciseQueryBuilder();
    query.setQuery(b.newNode("CM765").setAttribute("ChargeDeviceRef").toString());
    query.setParam("nested", "ChargeDeviceLocation.Address.PostTown:Peterborough");
    query.setRequestHandler("tree");

    long found = this.search(query).getNumFound();
    assertEquals(1, found);
  }

  /**
   * Check that document boost is properly applied.
   */
  @Test
  public void testDocumentBoost() throws QueryNodeException, IOException, SolrServerException {
    String input = "{ \"id\" : \"1\", \"aaa\" :  \"bbb\" }";
    NamedList params = new NamedList();
    params.add(SirenUpdateRequest.BOOST_PARAM, 1.0);

    this.sendUpdateRequest(input, SolrParams.toSolrParams(params));

    input = "{ \"id\" : \"2\", \"aaa\" :  \"bbb\" }";
    params = new NamedList();
    params.add(SirenUpdateRequest.BOOST_PARAM, 2.0);

    this.sendUpdateRequest(input, SolrParams.toSolrParams(params));

    input = "{ \"id\" : \"3\", \"aaa\" :  \"bbb\" }";
    params = new NamedList();
    params.add(SirenUpdateRequest.BOOST_PARAM, 1.5);

    this.sendUpdateRequest(input, SolrParams.toSolrParams(params));

    this.commit();

    SolrQuery query = new SolrQuery();
    final ConciseQueryBuilder b = new ConciseQueryBuilder();
    query.setQuery(b.newNode("bbb").setAttribute("aaa").toString());
    query.setRequestHandler("tree");
    SolrDocumentList results = this.search(query);
    assertEquals(3, results.size());
    assertEquals("2", results.get(0).get("id"));
    assertEquals("3", results.get(1).get("id"));
    assertEquals("1", results.get(2).get("id"));
  }

}

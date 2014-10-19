package org.apache.solr.client.solrj.streaming;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.client.solrj.impl.*;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.List;
import java.util.ArrayList;

/**
 *  All base tests will be done with CloudSolrStream. Under the covers CloudSolrStream uses SolrStream so
 *  SolrStream will get fully exercised through these tests.
 *
 **/

@Slow
public class StreamingTest extends AbstractFullDistribZkTestBase {

  private static final String SOLR_HOME = getFile("solrj" + File.separator + "solr").getAbsolutePath();

  @BeforeClass
  public static void beforeSuperClass() {
    AbstractZkTestCase.SOLRHOME = new File(SOLR_HOME());
  }

  @AfterClass
  public static void afterSuperClass() {

  }

  protected String getCloudSolrConfig() {
    return "solrconfig.xml";
  }

  @Override
  public String getSolrHome() {
    return SOLR_HOME;
  }

  public static String SOLR_HOME() {
    return SOLR_HOME;
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // we expect this time of exception as shards go up and down...
    //ignoreException(".*");

    System.setProperty("numShards", Integer.toString(sliceCount));
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    resetExceptionIgnores();
  }

  public StreamingTest() {
    super();
    sliceCount = 2;
    shardCount = 3;
  }

  @Override
  public void doTest() throws Exception {
    assertNotNull(cloudClient);

    handle.clear();
    handle.put("timestamp", SKIPVAL);

    waitForThingsToLevelOut(30);

    del("*:*");

    commit();

    SolrInputDocument doc1 = new SolrInputDocument();
    doc1.addField(id, "0");
    doc1.addField("a_s", "hello0");
    doc1.addField("a_i", "0");
    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.addField(id, "2");
    doc2.addField("a_s", "hello2");
    doc2.addField("a_i", "2");
    SolrInputDocument doc3 = new SolrInputDocument();
    doc3.addField(id, "3");
    doc3.addField("a_s", "hello3");
    doc3.addField("a_i", "3");
    SolrInputDocument doc4 = new SolrInputDocument();
    doc4.addField(id, "4");
    doc4.addField("a_s", "hello4");
    doc4.addField("a_i", "4");
    SolrInputDocument doc5 = new SolrInputDocument();
    doc5.addField(id, "1");
    doc5.addField("a_s", "hello1");
    doc5.addField("a_i", "1");

    UpdateRequest request = new UpdateRequest();
    request.add(doc1);
    request.add(doc2);
    request.add(doc3);
    request.add(doc4);
    request.add(doc5);

    request.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, false);
    cloudClient.request(request);
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    commit();

    String zkHost = zkServer.getZkAddress();

    //First test
    Map params = new HashMap();

    params.put("q","*:*");
    params.put("fl", "id,a_s,a_i");
    params.put("sort","a_i desc");

    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    CountStream cstream = new CountStream(stream, "count");
    cstream.open();
    List<Tuple> tuples = new ArrayList();
    for(Tuple t = cstream.read(); !t.EOF; t = cstream.read()) {
      tuples.add(t);
    }
    cstream.close();
    long count = cstream.longValue();

    assert(count == tuples.size());
    assertOrder(tuples, 4,3,2,1,0);

    del("*:*");
    commit();
  }

  public boolean assertOrder(List<Tuple> tuples, int... ids) throws Exception {
    int i = 0;
    for(int val : ids) {
      Tuple t = tuples.get(i);
      Long tip = (Long)t.get("id");
      if(tip.intValue() != val) {
        throw new Exception("Found value:"+t+" expecting:"+val);
      }
      ++i;
    }
    return false;
  }

  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = getDoc(fields);
    indexDoc(doc);
  }
}

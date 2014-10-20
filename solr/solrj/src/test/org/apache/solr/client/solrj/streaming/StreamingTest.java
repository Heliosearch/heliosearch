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
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.ArrayList;

/**
 *  All base tests will be done with CloudSolrStream. Under the covers CloudSolrStream uses SolrStream so
 *  SolrStream will get fully exercised through these tests.
 *
 **/

@Slow
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class StreamingTest extends AbstractFullDistribZkTestBase {

  private static final String SOLR_HOME = getFile("solrj" + File.separator + "solr").getAbsolutePath();

  static {
    schemaString = "schema-streaming.xml";
  }

  @BeforeClass
  public static void beforeSuperClass() {
    AbstractZkTestCase.SOLRHOME = new File(SOLR_HOME());
  }

  @AfterClass
  public static void afterSuperClass() {

  }

  protected String getCloudSolrConfig() {
    return "solrconfig-streaming.xml";
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

  private void testUniqueStream() throws Exception {

    //Test CloudSolrStream and UniqueStream

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();


    String zkHost = zkServer.getZkAddress();

    Map params = mapParams("q","*:*","fl","id,a_s,a_i,a_f","sort", "a_f asc,a_i asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    UniqueStream ustream = new UniqueStream(stream, new AscFieldComp("a_f"));
    CountStream cstream = new CountStream(ustream, "count");
    List<Tuple> tuples = getTuples(cstream);
    Long count = cstream.longValue();
    assert(count == 4);
    assertOrder(tuples, 0,1,3,4);

    del("*:*");
    commit();

  }

  private void testRankStream() throws Exception {


    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();

    String zkHost = zkServer.getZkAddress();

    Map params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    RankStream rstream = new RankStream(stream, 3, new DescFieldComp("a_i"));
    List<Tuple> tuples = getTuples(rstream);


    assert(tuples.size() == 3);
    assertOrder(tuples, 4,3,2);

    del("*:*");
    commit();
  }


  private void testSumStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();

    //Test CloudSolrStream and SumStream over an int field
    String zkHost = zkServer.getZkAddress();

    Map params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    SumStream sstream = new SumStream(stream, "a_i", "count", false);
    List<Tuple> tuples = getTuples(sstream);

    long sum = sstream.longValue();
    assert(sum == 10);
    assertOrder(tuples, 0,1,2,3,4);

    del("*:*");
    commit();

  }

  private void testCountStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();

    //Test CloudSolrStream and SumStream over an int field
    String zkHost = zkServer.getZkAddress();

    Map params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    CountStream cstream = new CountStream(stream, "count");
    List<Tuple> tuples = getTuples(cstream);

    long count = cstream.longValue();
    assert(count == 5);
    assertOrder(tuples, 0,1,2,3,4);

    del("*:*");
    commit();

  }


  private void testFilterStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();

    //Test CloudSolrStream and SumStream over an int field
    String zkHost = zkServer.getZkAddress();

    Map paramsA = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_s asc");
    CloudSolrStream streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    Map paramsB = mapParams("q","id:(0 2)","fl","a_s","sort", "a_s asc");
    CloudSolrStream streamB = new CloudSolrStream(zkHost, "collection1", paramsB);


    FilterStream fstream = new FilterStream(streamA, streamB, new AscFieldComp("a_s"));
    List<Tuple> tuples = getTuples(fstream);

    assert(tuples.size() == 2);
    assertOrder(tuples, 0,2);

    del("*:*");
    commit();
  }

  private void testParallelStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();

    String zkHost = zkServer.getZkAddress();

    Map paramsA = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_s asc", "partitionKeys","a_s");
    CloudSolrStream streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    Map paramsB = mapParams("q","id:(0 2)","fl","a_s","sort", "a_s asc", "partitionKeys","a_s");
    CloudSolrStream streamB = new CloudSolrStream(zkHost, "collection1", paramsB);

    FilterStream fstream = new FilterStream(streamA, streamB, new AscFieldComp("a_s"));
    ParallelStream pstream = new ParallelStream(zkHost,"collection1", fstream, 2, new AscFieldComp("a_s"));
    List<Tuple> tuples = getTuples(pstream);

    assert(tuples.size() == 2);
    assertOrder(tuples, 0,2);

    del("*:*");
    commit();
  }



  @Override
  public void doTest() throws Exception {
    assertNotNull(cloudClient);

    handle.clear();
    handle.put("timestamp", SKIPVAL);

    waitForThingsToLevelOut(30);

    del("*:*");

    commit();

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();

    String zkHost = zkServer.getZkAddress();
    Map params = null;

    //Basic CloudSolrStream Test

    params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i desc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    List<Tuple> tuples = getTuples(stream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 4, 3, 2, 1, 0);

    del("*:*");
    commit();

    testUniqueStream();
    testSumStream();
    testCountStream();
    testRankStream();
    testFilterStream();
    testParallelStream();
  }

  protected Map mapParams(String... vals) {
    Map params = new HashMap();
    String k = null;
    for(String val : vals) {
      if(k == null) {
        k = val;
      } else {
        params.put(k, val);
        k = null;
      }
    }

    return params;
  }

  protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    List<Tuple> tuples = new ArrayList();
    for(Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
      tuples.add(t);
    }
    tupleStream.close();
    return tuples;
  }

  protected boolean assertOrder(List<Tuple> tuples, int... ids) throws Exception {
    int i = 0;
    for(int val : ids) {
      Tuple t = tuples.get(i);
      Long tip = (Long)t.get("id");
      if(tip.intValue() != val) {
        throw new Exception("Found value:"+tip.intValue()+" expecting:"+val);
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

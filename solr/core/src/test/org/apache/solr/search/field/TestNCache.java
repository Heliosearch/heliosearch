package org.apache.solr.search.field;

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

import org.apache.lucene.index.IndexWriter;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.function.FuncValues;
import org.apache.solr.search.function.ValueSource;
import org.junit.BeforeClass;
import org.noggit.JSONUtil;
import org.noggit.ObjectBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

public class TestNCache extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml","schema15.xml");
  }


  String big;
  private void addDoc(int num) {
    String id = Integer.toString(num);
    if (num <=0) {
      assertU(adoc("id", id));
    } else {
      assertU(adoc("id", id, "val_i", id, "val_s1", id, "big_s1", big + id));
    }
  }

  public void testBasicSort() throws Exception {
    // test large values...
    int sz = IndexWriter.MAX_TERM_LENGTH;  // currently 32768-2
    StringBuilder sb = new StringBuilder(sz);
    for (int i=0; i<sz-1; i++) {
      sb.append("Z");
    }
    big = sb.toString();

    clearIndex();
    addDoc(5);
    addDoc(1);
    assertU(commit());
    addDoc(-1);    // empty segment for fiedcache
    addDoc(-2);
    assertU(commit());
    addDoc(-3);
    addDoc(3);
    assertU(commit());

    assertJQ(req("q", "{!frange l=1 u=2}val_s1", "sort", "val_s1 desc", "fl", "id")
        ,  "/response/docs==[{'id':'1'}]"
    );

    String desc = "/response/docs==[{'id':'5'},{'id':'3'},{'id':'1'},{'id':'-1'},{'id':'-2'},{'id':'-3'}]";


    // assertQ(req("q","*:*", "facet","true", "facet.field","val_i1"));
    assertJQ(req("q", "*:*", "sort", "val_i desc", "fl", "id")
        , desc
    );

    assertJQ(req("q","*:*", "sort","val_s1 desc", "fl","id")
        , desc
    );

    assertJQ(req("q", "*:*", "sort", "big_s1 desc", "fl", "id")
        , desc
    );


    // make sure deleteByQuery works OK
    assertU(delQ("{!frange l=1 u=2}val_s1"));
    assertU(commit());
    assertJQ(req("q", "{!frange l=1 u=2}val_s1", "sort", "val_s1 desc", "fl", "id")
        ,  "/response/docs==[]"
    );


  }

  public void testSize() throws Exception {
    clearIndex();
    String s = "12345678901234567890";
    int nSets = 10;

    int docsPerSet = 2;
    int termSize = 0;
    for (int i=0; i<nSets; i++) {
      String id = String.format(Locale.ROOT, "%08d", i);
      assertU(adoc("id", id,     "val_s1", id));
      termSize += 1 + id.length();
      assertU(adoc("id", id+"a", "val_s1", id+s));
      int sz = id.length() + s.length();
      termSize += ((sz <= 0x7f) ? 1 : 2);
      termSize += sz;
    }

    int nDocs = docsPerSet * nSets;
    int nTerms = nDocs;

    assertU(optimize());

    SolrQueryRequest req = req();
    SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, new SolrQueryResponse()));

    SchemaField sf = req.getSchema().getField("val_s1");
    ValueSource vs = sf.getType().getValueSource(sf, null);
    QueryContext qcontext = QueryContext.newContext(req.getSearcher());
    vs.createWeight(qcontext);

    FuncValues funcValues = vs.getValues(qcontext, req.getSearcher().getTopReaderContext().leaves().get(0));
    assertTrue(funcValues instanceof StrArrLeafValues);

    StrArrLeafValues vals = (StrArrLeafValues)funcValues;

    assertTrue(vals._getDocToOrdArray() instanceof LongArray8);
    assertTrue(vals._getOrdToOffsetArray().memSize() == nTerms*1);

    long estSize =
        (nDocs * 1) // docToOrd array
       +(nTerms * 1) // ordToOffset array
       +(termSize);  // term bytes

    long reportedSize = vals.getSizeInBytes();

    assertTrue(estSize >= reportedSize);

    req.close();
    SolrRequestInfo.clearRequestInfo();
  }



  public static class LongSpecial extends LVals {
    static long x = 0x8000000000000000L;
    static long y = 0x7fffffffffffffffL;
    // for example, the middle special values (around 0) are -256, -128, 0, 127, 255
    static long[] special = new long[] {x, x>>1, x>>(32-1), x>>(32), x>>(32+16-1), x>>(32+16), x>>(32+16+8-1), x>>(32+16+8), 0, y>>(32+16+8), y>>(32+16+8-1), y>>(32+16), y>>(32+16-1), y>>(32), y>>(32-1), y>>1, y};

    long low;
    long high;

    public static LongSpecial create() {
      Random r = random();
      int lowIdx = random().nextInt(special.length - 1);
      int highIdx = lowIdx + random().nextInt(special.length - lowIdx - 1) + 1;

      long low = special[lowIdx];
      long high = special[highIdx];

      if (r.nextInt(100) < 25) {
        // go over the boundary 25% of the time
        long newLow = low - (r.nextInt(2)+1);
        if (newLow < low) {
          low = newLow;
        }
      }

      if (r.nextInt(100) < 25) {
        // go over the boundary 25% of the time
        long newHigh = high + (r.nextInt(2)+1);
        if (newHigh > high) {
          high = newHigh;
        }
      }

      return new LongSpecial(low, high);
    }

    public LongSpecial(long low, long high) {
      this.low = low;
      this.high = high;
    }

    @Override
    public long getLong() {
      Random r = random();
      long v = 0;
      int cases = 3;
      if (low <= 0 && high >= 0) {
        cases++;
      }

      switch (r.nextInt(cases)) {
        case 0: {
          int i = r.nextInt(5);
          v = low + i;
          break;
        }
        case 1: {
          int i = r.nextInt(5);
          v = high - i;
          break;
        }
        case 2: {
          long range=high-low+1;
          if (range <= 0) {
            v = r.nextLong();
          } else {
            v = low + ((r.nextLong() & 0x7fffffffffffffffL) % range);
          }
          break;
        }
        case 3: {
          // if the range includes 0, then generate some small values around that
          v = r.nextInt(5)-2;
          break;
        }
      }

      return v;
    }

    @Override
    public Comparable get() {
      return getLong();
    }
  }


  public static class IntSpecial extends IVals {
    static int x = 0x80000000;
    static int y = 0x7fffffff;
    // for example, the middle special values (around 0) are -256, -128, 0, 127, 255
    static int[] special = new int[] {x, x>>1, x>>(32+16-1), x>>(32+16), x>>(32+16+8-1), x>>(32+16+8), 0, y>>(32+16+8), y>>(32+16+8-1), y>>(32+16), y>>(32+16-1), y>>1, y};

    int low;
    int high;

    public static LongSpecial create() {
      Random r = random();
      int lowIdx = random().nextInt(special.length - 1);
      int highIdx = lowIdx + random().nextInt(special.length - lowIdx - 1) + 1;

      int low = special[lowIdx];
      int high = special[highIdx];

      if (r.nextInt(100) < 25) {
        // go over the boundary 25% of the time
        int newLow = low - (r.nextInt(2)+1);
        if (newLow < low) {
          low = newLow;
        }
      }

      if (r.nextInt(100) < 25) {
        // go over the boundary 25% of the time
        int newHigh = high + (r.nextInt(2)+1);
        if (newHigh > high) {
          high = newHigh;
        }
      }

      return new LongSpecial(low, high);
    }

    public IntSpecial(int low, int high) {
      this.low = low;
      this.high = high;
    }

    @Override
    public int getInt() {
      Random r = random();
      int v = 0;
      int cases = 3;
      if (low <= 0 && high >= 0) {
        cases++;
      }

      switch (r.nextInt(cases)) {
        case 0: {
          int i = r.nextInt(5);
          v = low + i;
          break;
        }
        case 1: {
          int i = r.nextInt(5);
          v = high - i;
          break;
        }
        case 2: {
          long range=high-low+1;
           v = (int)( low + ((r.nextLong() & 0x7fffffffffffffffL) % range) );
          break;
        }
        case 3: {
          // if the range includes 0, then generate some small values around that
          v = r.nextInt(5)-2;
          break;
        }
      }

      return v;
    }

    @Override
    public Comparable get() {
      return getInt();
    }
  }




  private static String longFieldName(int fieldNum) {
    return "f"+fieldNum+"_l";
  }
  private static String intFieldName(int fieldNum) {
    return "f"+fieldNum+"_i";
  }


  public void testCache() throws Exception {
    Random r = random();

    int indexIter=20;
    int updateIter=10;


    long nLongFields = 10;
    long nIntFields = 10;

    Map<Comparable, Doc> model = null;

    while (--indexIter >= 0) {
      List<FldType> types = new ArrayList<FldType>();
      types.add(new FldType("id",ONE_ONE, new SVal('A','Z',4,4)));
      types.add(new FldType("score_f",ONE_ONE, new FVal(1,100)));  // field used to score
      types.add(new FldType("sparse_f",ZERO_ONE, new FVal(-100,100)));  // field used to score
      types.add(new FldType("sparse_d",ZERO_ONE, new DVal(-1000,1000)));  // field used to score
      types.add(new FldType("small_s1",ZERO_ONE, new SVal('a','z',1,1)));
      types.add(new FldType("big_s1",ZERO_ONE, new SVal('A','z',1,30000)));
      types.add(new FldType("bigb_s1",ZERO_ONE, new SVal('A','z',1,2)));
      types.add(new FldType("bigc_s1",ZERO_ONE, new SVal('A','z',1,3)));
      types.add(new FldType("bigd_s1",ZERO_ONE, new SVal('A','z',1,4)));

      StringBuilder sb = new StringBuilder();
      for (int i=0; i<nLongFields; i++) {
        String fname = longFieldName(i);
        types.add(new FldType(fname, r.nextBoolean() ?  ZERO_ONE : ONE_ONE, LongSpecial.create()));
        if (i != 0) sb.append(',');
        sb.append(fname).append(':').append("field(").append(fname).append(')');  // access the field via fieldcache...  myfield:field(myfield)
      }
      String flArg = sb.toString();

      sb = new StringBuilder();
      for (int i=0; i<nIntFields; i++) {
        String fname = intFieldName(i);
        types.add(new FldType(fname, r.nextBoolean() ?  ZERO_ONE : ONE_ONE, IntSpecial.create()));
        if (i != 0) sb.append(',');
        sb.append(fname).append(':').append("field(").append(fname).append(')');  // access the field via fieldcache...  myfield:field(myfield)
      }
      String flArg2 = sb.toString();


      clearIndex();
      model = null;

      for (int upIter=0; upIter<updateIter; upIter++) {
        int indexSize = random().nextInt(50) + 1;  // something big enough to get us over 128 / 256 (on later iterations) to exercize more ords than that...
        model = indexDocs(types, model, indexSize);

        // System.out.println("MODEL=" + model);

        int rows=model.size();


        //
        // create model response
        //
        List<Doc> docList = new ArrayList<Doc>(model.values());
        Collections.sort(docList, createComparator("_docid_",true,false,false,false));
        List sortedDocs = new ArrayList();
        for (Doc doc : docList) {
          if (sortedDocs.size() >= rows) break;
          sortedDocs.add(doc.toObject(h.getCore().getLatestSchema()));
        }
        Object modelDocs = sortedDocs;


        //
        // get solr response
        //
        SolrQueryRequest req = req("wt","json","indent","true", "echoParams","all"
            ,"q","*:*"
            ,"rows",""+rows
            ,"fl","id:field(id), score_f:field(score_f)"
            ,"fl","sparse_f:field(sparse_f)"
            ,"fl","sparse_d:field(sparse_d)"
            ,"fl","small_s1:field(small_s1)"
            ,"fl","big_s1:field(big_s1)"
            ,"fl","bigb_s1:field(bigb_s1)"
            ,"fl","bigc_s1:field(bigc_s1)"
            ,"fl","bigd_s1:field(bigd_s1)"
            ,"fl",flArg
            ,"fl",flArg2
        );

        String strResponse = h.query(req);


        Object realResponse = ObjectBuilder.fromJSON(strResponse);
        String err = JSONTestUtil.matchObj("/response/docs", realResponse, modelDocs);
        if (err != null) {
          log.error("JOIN MISMATCH: " + err
              + "\n\trequest="+req
              + "\n\tresult="+strResponse
              + "\n\texpected="+ JSONUtil.toJSON(modelDocs)
              + "\n\tmodel="+ model
          );

          // re-execute the request... good for putting a breakpoint here for debugging
          String rsp = h.query(req);

          fail(err);
        }




        ///////////////////
        //now test sorting




      }

    }

  }
}

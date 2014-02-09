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

import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.noggit.JSONUtil;
import org.noggit.ObjectBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TestNCache extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml","schema15.xml");
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

    int indexIter=10;
    int updateIter=10;
    int queryIter=50;


    long nLongFields = 20;
    long nIntFields = 20;

    Map<Comparable, Doc> model = null;

    while (--indexIter >= 0) {
      List<FldType> types = new ArrayList<FldType>();
      types.add(new FldType("id",ONE_ONE, new SVal('A','Z',4,4)));
      types.add(new FldType("score_f",ONE_ONE, new FVal(1,100)));  // field used to score

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
        int indexSize = random().nextInt(20);
        model = indexDocs(types, model, indexSize);

        // System.out.println("MODEL=" + model);

        int rows=1000000;

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
            ,"fl","id, score_f"
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

      }

    }

  }
}

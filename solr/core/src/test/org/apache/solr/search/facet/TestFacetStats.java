package org.apache.solr.search.facet;

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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

@LuceneTestCase.SuppressCodecs({"Lucene3x","Lucene40","Lucene41","Lucene42","Appending","Asserting"})
public class TestFacetStats extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-tlog.xml","schema_latest.xml");
  }

  @Test
  public void testStats() throws Exception {
    assertU(add(doc("id", "1","cat_s", "A", "where_s","NY", "num_d", "4",  "num_i","2",   "val_b","true")));
    assertU(add(doc("id", "2","cat_s", "B", "where_s","NJ", "num_d", "-9", "num_i","-5",  "val_b","false")));
    assertU(add(doc("id", "3")));
    assertU(commit());
    assertU(add(doc("id", "4", "cat_s", "A", "where_s","NJ", "num_d", "2", "num_i", "3")));
    assertU(add(doc("id", "5","cat_s", "B",  "where_s","NJ", "num_d", "11", "num_i","7")));
    assertU(commit());
    assertU(add(doc("id", "6","cat_s", "B",  "where_s","NY", "num_d", "-5", "num_i","-5")));
    assertU(commit());


    // sort desc
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field","cat_s"
        ,"facet.stat", "x:sum(num_d)"
        ,"facet.sort", "x desc"
    )
        ,"facets/cat_s=={'stats':{'x':3.0}, 'buckets':[{'val':'A','x':6.0},{'val':'B','x':-3.0}] }"
    );

    // sort asc
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field","cat_s"
        ,"facet.stat", "x:sum(num_d)"
        ,"facet.sort", "x asc"
    )
        ,"facets/cat_s=={'stats':{'x':3.0}, 'buckets':[{'val':'B','x':-3.0},{'val':'A','x':6.0}] }"
    );

    // test two
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field","cat_s"
        ,"facet.stat", "x:sum(num_d)"
        ,"facet.stat", "y:avg(num_d)"
        ,"facet.sort", "x desc"
    )
        ,"facets/cat_s=={'stats':{'x':3.0,'y':0.6}, 'buckets':[{'val':'A','x':6.0,'y':3.0},{'val':'B','x':-3.0,'y':-1.0}] }"
    );

    // bool field, sort desc
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field","val_b"
        ,"facet.stat", "x:sum(num_d)"
        ,"facet.sort", "x desc"
    )
        ,"facets/val_b=={ 'stats':{ 'x':-5.0}, 'buckets':[{ 'val':true, 'x':4.0}, { 'val':false, 'x':-9.0}]} "
    );

    /*** TODO - finish facet.version support
    // test facet.version
    assertJQ(req("q","*:*", "rows","0", "facet","true"
            ,"facet.field","cat_s"
            ,"facet.version","2"
        )
        ,"facets/cat_s=={'stats':{'x':3.0,'y':0.6}, 'buckets':[{'val':'A','x':6.0,'y':3.0},{'val':'B','x':-3.0,'y':-1.0}] }"
    );
    ***/

  }


  @Test
  public void testMultipleFields() throws Exception {

                    // 1      2     3     4     5      6
    String[] svals=  {"A",   "B",  null, "A",  "B",   "B"};
    String[] ssvals= {"X Y", "X",  null, "Y",  "X Y", "Y"};
    String[] nvals=  {"4",   "-9", null, "2",  "11",  "-5"};

    String[] numbers = { "num_d", "num_f", "num_l", "num_i",
                        "num_dd", "num_fd", "num_ld", "num_id"  // docvalues
    };
    String[] strings =  { "val_s", "val_ss", "val_sd",
        // "val_sds" TODO
    };
    String[] sstrings = { "multi_ss",
        // "multi_sds" TODO
    };

    for (int i=0; i<svals.length; i++) {
      SolrInputDocument sdoc = new SolrInputDocument();
      sdoc.addField("id", Integer.toString(i+1));
      sdoc.addField("id_ss", Integer.toString(i+1));
      sdoc.addField("id_i", i+1);

      for (String strField : strings) {
        sdoc.addField(strField, svals[i]);
      }

      for (String strField : sstrings) {
        if (ssvals[i] == null) break;

        String[] vals = ssvals[i].split(" ");
        for (String val : vals) {
          sdoc.addField(strField, val);
        }
      }

      for (String numberField : numbers) {
        sdoc.addField(numberField, nvals[i]);
      }

      do {
        assertU(adoc(sdoc));
      } while (random().nextInt(100) < 25);  // add duplicates

      if (random().nextInt(100) < 20) {  // make multiple segments
        assertU(commit());
      }
    }
    assertU(commit());

    // try all combinations of different field types
    for (String s : strings) {
      s1 = s;
      for (String ss : sstrings) {
        ss1 = ss;
        for (String n : numbers) {
          n1 = n;
          doFieldType();
        }
      }
    }


  }


  private String call(String... args) {
    StringBuilder sb = new StringBuilder();
    sb.append('(');
    boolean first = true;
    for (String arg : args) {
      if (first) {
        first = false;
      } else {
        sb.append(',');
      }
      sb.append(arg);
    }
    sb.append(')');
    return sb.toString();
  }

  private boolean isFloat(String field) {
    return (field.contains("_f") || field.contains("_d"));
  }

  String s1;
  String ss1;
  String n1;


  public void doFieldType() throws Exception {
    // sort desc
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "x:sum"+ call(n1)
        ,"facet.sort", "x desc"
    )
        ,"facets/s1=={'stats':{'x':3.0}, 'buckets':[{'val':'A','x':6.0},{'val':'B','x':-3.0}] }"
    );

    // test offset
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "x:sum"+ call(n1)
        ,"facet.sort", "x desc"
        ,"facet.offset", "1"
    )
        ,"facets/s1=={'stats':{'x':3.0}, 'buckets':[ {'val':'B','x':-3.0}] }"
    );

    // test limit
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "x:sum"+ call(n1)
        ,"facet.sort", "x desc"
        ,"facet.limit", "1"
    )
        ,"facets/s1=={'stats':{'x':3.0}, 'buckets':[ {'val':'A','x':6.0} ] }"
    );

    // test no limit
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "x:sum"+ call(n1)
        ,"facet.sort", "x desc"
        ,"facet.limit", "-1"
    )
        ,"facets/s1=={'stats':{'x':3.0}, 'buckets':[{'val':'A','x':6.0},{'val':'B','x':-3.0}] }"
    );

    // sort index order
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "x:sum"+ call(n1)
        ,"facet.sort", "index"
    )
        ,"facets/s1=={'stats':{'x':3.0}, 'buckets':[{'val':'A','x':6.0},{'val':'B','x':-3.0}] }"
    );

    // sort asc
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "x:sum"+ call(n1)
        ,"facet.sort", "x asc"
    )
        ,"facets/s1=={'stats':{'x':3.0}, 'buckets':[{'val':'B','x':-3.0},{'val':'A','x':6.0}] }"
    );

    // test multiple
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "x:sum"+ call(n1)
        ,"facet.stat", "y:avg"+ call(n1)
        ,"facet.sort", "x desc"
    )
        ,"facets/s1=={'stats':{'x':3.0,'y':0.6}, 'buckets':[{'val':'A','x':6.0,'y':3.0},{'val':'B','x':-3.0,'y':-1.0}] }"
    );

    // test including count
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "x:sum"+ call(n1)
        ,"facet.stat", "count"
        ,"facet.sort", "x desc"
    )
        ,"facets/s1=={'stats':{'x':3.0, 'count':5}, 'buckets':[{'val':'A','x':6.0, 'count':2},{'val':'B','x':-3.0, 'count':3}] }"
    );

    // test as part of a bigger function
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "x:sum(mul" + call(n1,"10") + ")"
        ,"facet.sort", "x desc"
    )
        ,"facets/s1=={'stats':{'x':30.0}, 'buckets':[{'val':'A','x':60.0},{'val':'B','x':-30.0}] }"
    );

    // sort by count
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "count"  // special case... support bare "count" like we support bare facet.sort=count
        ,"facet.sort", "count desc"
    )
        ,"facets/s1=={'stats':{'count':5}, 'buckets':[{'val':'B','count':3},{'val':'A','count':2}] }"
    );

    // count asc
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "count"  // special case... support bare "count" like we support bare facet.sort=count
        ,"facet.sort", "count asc"
    )
        ,"facets/s1=={'stats':{'count':5}, 'buckets':[{'val':'A','count':2},{'val':'B','count':3}] }"
    );

    // default sort is by count, and it will be added to the bucket as well if not specified
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "x:sum"+ call(n1)
    )
        ,"facets/s1=={'stats':{'x':3.0, 'count':5}, 'buckets':[ {'val':'B','x':-3.0, 'count':3}, {'val':'A','x':6.0, 'count':2}] }"
    );

    // default label is the function
    String label = "'sum"+call(n1) + "'";
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "sum"+call(n1)
    )
        ,"facets/s1=={'stats':{"+label+":3.0, 'count':5}, 'buckets':[ {'val':'B',"+label+":-3.0, 'count':3}, {'val':'A',"+label+":6.0, 'count':2}] }"
    );

    // faceting on multivalued field
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + ss1
        ,"facet.stat", "x:sum"+ call(n1)
        ,"facet.sort", "x desc"
    )
        ,"facets/s1=={'stats':{'x':18.0}, 'buckets':[{'val':'Y','x':12.0},{'val':'X','x':6.0}] }"
    );


    // subfacet query
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "x:sum"+ call(n1)
        ,"facet.sort", "x desc"
        ,"subfacet.s1.query", "{!key=q}"+ss1+":X"
    )
        ,"facets/s1=={'stats':{'x':3.0}, 'buckets':[{'val':'A','x':6.0, 'q':{'x':4.0}},{'val':'B','x':-3.0, 'q':{'x':2.0}}] }"
    );

    // subfacet field multi-valued
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "x:sum"+ call(n1)
        ,"facet.sort", "x desc"
        ,"subfacet.s1.field", "{!key=f}"+ss1
    )
        ,"facets/s1=={'stats':{'x':3.0}, 'buckets':[" +
            "{'val':'A','x':6.0, " +
              "'f':{'stats':{ 'x':10.0}, 'buckets':[{'val':'Y','x':6.0},{'val':'X','x':4.0}]} }" +
           ",{'val':'B','x':-3.0, " +
              "'f':{'stats':{ 'x':8.0}, 'buckets':[{'val':'Y','x':6.0},{'val':'X','x':2.0}]} } ]}}"
    );

    // subfacet field (test format still uses buckets, defaults to count desc)
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + ss1
        ,"f.s1.facet.stat", "x:sum"+ call(n1)
        ,"f.s1.facet.sort", "x desc"
        ,"subfacet.s1.field", "{!key=f}"+s1
    )
        ,"facets/s1=={'stats':{'x':18.0}, 'buckets':[" +
        "{'val':'Y','x':12.0, " +
        "'f':{'stats':{ 'count':4}, 'buckets':[{'val':'A','count':2},{'val':'B','count':2}]} }" +
        ",{'val':'X','x':6.0, " +
        "'f':{'stats':{ 'count':3}, 'buckets':[{'val':'B','count':2},{'val':'A','count':1}]} } ]}}"
    );

    if (isFloat(n1)) {  // bucket values change between float/int for range faceting
      // range subfacet
      assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "x:sum"+ call(n1)
        ,"facet.sort", "x desc"
         ,"subfacet.s1.range", "{!key=r}"+n1
        ,"f.r.facet.range.start", "-5",   "f.r.facet.range.start","-10", "f.r.facet.range.end","5", "f.r.facet.range.gap","5", "f.r.facet.range.other","all"
        )
        ,"facets/s1=={ 'stats':{ 'x':3.0}, 'buckets':[" +
              "{ 'val':'A', 'x':6.0, 'r':{ 'buckets':[{ 'val':'-5.0', 'x':0.0}, { 'val':'0.0', 'x':6.0}], 'gap':5.0, 'start':-5.0, 'end':5.0, 'before':{ 'x':0.0}, 'after':{ 'x':0.0}, 'between':{ 'x':6.0}}}, " +
              "{ 'val':'B', 'x':-3.0, 'r':{ 'buckets':[{ 'val':'-5.0', 'x':-5.0}, { 'val':'0.0', 'x':0.0}], 'gap':5.0, 'start':-5.0, 'end':5.0, 'before':{ 'x':-9.0}, 'after':{ 'x':11.0}, 'between':{ 'x':-5.0}}}]} "
      );

      // range facet then subfield facet
      assertJQ(req("q","*:*", "rows","0", "facet","true"
          ,"facet.range", "{!key=r}"+n1
          ,"f.r.facet.range.start", "-5", "f.r.facet.range.end","5", "f.r.facet.range.gap","5", "f.r.facet.range.other","all"
          ,"subfacet.r.field", "{!key=s}" + ss1
          ,"facet.stat", "x:sum"+ call(n1)
          ,"facet.sort", "x desc"
      )
          ,"facets/r== { 'buckets':[{ 'val':'-5.0', 'x':-5.0, 's':{ 'stats':{ 'x':-5.0}, 'buckets':[{ 'val':'X', 'x':0.0}, { 'val':'Y', 'x':-5.0}]}}, " +
                                                       "{ 'val':'0.0', 'x':6.0, 's':{ 'stats':{ 'x':10.0}, 'buckets':[{ 'val':'Y', 'x':6.0}, { 'val':'X', 'x':4.0}]}}]," +
                                                       " 'gap':5.0, 'start':-5.0, 'end':5.0, " +
                                                       " 'before':{ 'x':-9.0, 's':{ 'stats':{ 'x':-9.0}, 'buckets':[{ 'val':'Y', 'x':0.0}, { 'val':'X', 'x':-9.0}]}}," +
                                                       " 'after':{ 'x':11.0, 's':{ 'stats':{ 'x':22.0}, 'buckets':[{ 'val':'X', 'x':11.0}, { 'val':'Y', 'x':11.0}]}}," +
                                                       " 'between':{ 'x':1.0, 's':{ 'stats':{ 'x':5.0}, 'buckets':[{ 'val':'X', 'x':4.0}, { 'val':'Y', 'x':1.0}]}}   }"
      );


      // facet by range, then by a query and a field
      assertJQ(req("q","*:*", "rows","0", "facet","true"
          ,"facet.range", "{!key=r}"+n1
          ,"f.r.facet.range.start", "-10",   "f.r.facet.range.end","10", "f.r.facet.range.gap","10"
          ,"subfacet.r.field", "{!key=s}" + s1             // r/s
          ,"subfacet.r.query", "{!key=q}" + "id:(1 2 3)"   // r/q
          ,"subfacet.s.field", "{!key=f2}" + ss1           // r/s/f2
          ,"subfacet.s.query", "{!key=q2}" + "id:(4 5)"    // r/s/q2
          ,"subfacet.q.field", "{!key=f3}" + ss1           // r/q/f3

          ,"facet.stat", "x:sum"+ call(n1)
          ,"facet.stat", "u:unique" + call(s1)
          ,"facet.sort", "x desc"
          ,"facet.limit", "1"  // cut down on the size of the response
          ,"facet.mincount", "0"
      )
          ,"facets/r=={ 'buckets':[" +
              "{ 'val':'-10.0', 'x':-14.0, 'u':1, 'q':{ 'x':-9.0, 'u':1, 'f3':{ 'stats':{ 'x':-9.0, 'u':1}, 'buckets':[{ 'val':'Y', 'x':0.0, 'u':0}]}}, " +
                "'s':{ 'stats':{ 'x':-14.0, 'u':1}, 'buckets':[{ 'val':'A', 'x':0.0, 'u':0, 'q2':{ 'x':0.0, 'u':0}, " +
                  "'f2':{ 'stats':{ 'x':0.0, 'u':0}, 'buckets':[{ 'val':'X', 'x':0.0, 'u':0}]}}]}}, " +
              "{ 'val':'0.0', 'x':6.0, 'u':1, 'q':{ 'x':4.0, 'u':1, 'f3':{ 'stats':{ 'x':8.0, 'u':1}, 'buckets':[{ 'val':'X', 'x':4.0, 'u':1}]}}, " +
                "'s':{ 'stats':{ 'x':6.0, 'u':1}, 'buckets':[{ 'val':'A', 'x':6.0, 'u':1, 'q2':{ 'x':2.0, 'u':1}, " +
                  "'f2':{ 'stats':{ 'x':10.0, 'u':1}, 'buckets':[{ 'val':'Y', 'x':6.0, 'u':1}]}}]}}], 'gap':10.0, 'start':-10.0, 'end':10.0}"

      //                    1      2      3    4     5      6
      // String[] svals=  {"A",   "B",  null, "A",  "B",   "B"};
      // String[] ssvals= {"X Y", "X",  null, "Y",  "X Y", "Y"};
      // String[] nvals=  {"4",   "-9", null, "2",  "11",  "-5"};

          // for num[0 TO 10] (docs 1,4) -> string:A (1,4) -> string:Y (1,4) -> sum(1,4)==6, unique(strfield,(1,4))=1
          // for num[-10 TO 0] (docs 2,6) -> ( string:A==[] string:B==[2,6])  // empty bucket A is selected at this point because it has a higher sum
      );



    }



    ///////////////////////// unique

    // multi-valued unique
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "x:unique"+ call(ss1)
        ,"facet.sort", "x desc"
    )
        ,"facets/s1=={'stats':{'x':2}, 'buckets':[{'val':'A','x':2},{'val':'B','x':2}] }"
    );

    // multi-valued unique
    assertJQ(req("q","id:(1 2)", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "x:unique"+ call(ss1)
        ,"facet.sort", "x desc"
    )
        ,"facets/s1=={'stats':{'x':2}, 'buckets':[{'val':'A','x':2},{'val':'B','x':1}] }"
    );

    // single_valued unique
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + ss1
        ,"facet.stat", "x:unique"+ call(s1)
        ,"facet.sort", "x desc"
    )
        ,"facets/s1=={'stats':{'x':2}, 'buckets':[{'val':'X','x':2},{'val':'Y','x':2}] }"
    );

    // single_valued unique
    assertJQ(req("q","id:(1 2)", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + ss1
        ,"facet.stat", "x:unique"+ call(s1)
        ,"facet.sort", "x desc"
    )
        ,"facets/s1=={'stats':{'x':2}, 'buckets':[{'val':'X','x':2},{'val':'Y','x':1}] }"
    );

    // multi-valued unique (on id_ss, to ensure we exercise non-big terms)
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "x:unique"+ call("id_ss")
        ,"facet.sort", "x desc"
    )
        ,"facets/s1=={'stats':{'x':5}, 'buckets':[{'val':'B','x':3},{'val':'A','x':2}] }"
    );

    // multi-valued facet (on id_ss, to ensure we exercise non-big terms)
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + "id_ss"
        ,"facet.stat", "x:sum"+ call(n1)
        ,"facet.sort", "x desc"
    )
        ,"facets/s1=={ 'stats':{ 'x':3.0}, 'buckets':[{ 'val':'5', 'x':11.0}, { 'val':'1', 'x':4.0}, { 'val':'4', 'x':2.0}, { 'val':'3', 'x':0.0}, { 'val':'6', 'x':-5.0}, { 'val':'2', 'x':-9.0}]}"
    );

    // min, max
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "min:min"+ call(n1)
        ,"facet.stat", "max:max"+ call(n1)
        ,"facet.sort", "max desc"
    )
        ,"facets/s1=={ 'stats':{ 'min':-9.0, 'max':11.0}, 'buckets':[{ 'val':'B', 'min':-9.0, 'max':11.0}, { 'val':'A', 'min':2.0, 'max':4.0}]}"
    );

    // min,max with missing values
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.query", "{!key=q}id:(1 3)"  // min of 4.0, null should be 4.0
        ,"facet.stat", "min:min"+ call(n1)
    )
        ,"facets/q=={'min':4.0}"
    );
    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.query", "{!key=q}id:(2 3)"   // max of -9.0, null should be -9.0
        ,"facet.stat", "max:max"+ call(n1)
    )
        ,"facets/q=={'max':-9.0}"
    );

    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field", "{!key=s1}" + s1
        ,"facet.stat", "sumsq:sumsq"+ call(n1)
        ,"facet.sort", "sumsq desc"
    )
        ,"facets/s1=={ 'stats':{ 'sumsq':247.0}, 'buckets':[{ 'val':'B', 'sumsq':227.0}, { 'val':'A', 'sumsq':20.0}]} "
    );

  }


  /*** just for manual tests
  @Test
  public void testStuff() throws Exception {
    assertU(add(doc("id", "1","cat_s", "A", "where_s","NY", "num_d", "4",  "num_i","2",  "val_t", "X Y")));
    assertU(add(doc("id", "2", "cat_s", "B", "where_s", "NJ", "num_d", "-9", "num_i", "-5", "val_t", "X")));
    assertU(add(doc("id", "3")));
    assertU(commit());
    assertU(add(doc("id", "4", "cat_s", "A", "where_s","NJ", "num_d", "2", "num_i", "3")));
    assertU(add(doc("id", "5", "cat_s", "B", "where_s", "NJ", "num_d", "11", "num_i", "7")));
    assertU(commit());
    assertU(add(doc("id", "6", "cat_s", "B", "where_s", "NY", "num_d", "-3", "num_i", "-5")));
    assertU(commit());


    assertJQ(req("q","id:(1 2 6)", "rows","0", "facet","true"
        ,"facet.field","cat_s"
        ,"facet.stat", "x:unique(val_t)"
        ,"facet.sort", "x asc"
    )
        ,"/foobar=="
    );

    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field","cat_s"
        ,"facet.stat", "x:sum(num_d)"
        ,"subfacet.cat_s.field", "where_s"
        ,"subfacet.where_s.query", "num_i:[0 TO 10]"
    )
        ,"/foobar=="
    );


    assertJQ(req("q","*:*", "rows","0", "facet","true"
        ,"facet.field","cat_s"
        ,"facet.stat", "x:sum(num_d)"
        ,"facet.stat", "y:sum(num_d)"
        ,"facet.sort", "count"

    )
        ,"/response/facet_counts=="
    );

    assertJQ(req("q","*:*", "rows","0", "facet","true",
        "facet.field","cat_s",
        "facet.stat","x:sum(num_d)",
        "facet.stat","y:sum(num_i)",
        "facet.sort","x",
        "facet.query", "{!key=q1}id:(1 2 3)",
        "facet.range","num_i", "facet.range.start","-10", "facet.range.end","10", "facet.range.gap","10", "facet.range.other","all",
        "f.q1.facet.stat", "z:sum(num_d)",
        "subfacet.q1.query", "id:1",
        "subfacet.cat_s.field", "{!key=where}where_s",
        "subfacet.cat_s.query", "where_s:NJ",
        "subfacet.num_i.field", "{!key=where}where_s",
        "subfacet.num_i.query", "where_s:NJ",
        "subfacet.num_i.range", "numd"
    )
        ,"/foobar"
    );

  }
   ***/


}

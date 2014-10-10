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
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.macro.MacroExpander;
import org.junit.BeforeClass;
import org.junit.Test;

@LuceneTestCase.SuppressCodecs({"Lucene3x","Lucene40","Lucene41","Lucene42","Appending","Asserting"})
public class TestJsonFacets extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-tlog.xml","schema_latest.xml");
  }

  // nocommit - just scratch space for debugging failing cases...
  @Test
  public void testStats() throws Exception {
    assertU(add(doc("id", "1", "cat_s", "A", "where_s", "NY", "num_d", "4", "num_i", "2", "val_b", "true")));
    assertU(add(doc("id", "2", "cat_s", "B", "where_s", "NJ", "num_d", "-9", "num_i", "-5", "val_b", "false")));
    assertU(add(doc("id", "3")));
    assertU(commit());
    assertU(add(doc("id", "4", "cat_s", "A", "where_s", "NJ", "num_d", "2", "num_i", "3")));
    assertU(add(doc("id", "5", "cat_s", "B", "where_s", "NJ", "num_d", "11", "num_i", "7")));
    assertU(commit());
    assertU(add(doc("id", "6", "cat_s", "B", "where_s", "NY", "num_d", "-5", "num_i", "-5")));
    assertU(commit());


    // terms facet with nested field facet
    assertJQ(req("q", "*:*", "rows", "0",
            "facet","true"  // currently still needed
            , "json.facet", "{cat:{terms:{field:'cat_s', facet:{nj:{query:'where_s:NJ'}}    }   }} }"
        )
        , "facets=="
    );
  }

  @Test
  public void testStatsTemplated() throws Exception {
    // single valued strings
    doStatsTemplated( params("facet","true", "rows","0",  "cat_s","cat_s", "where_s","where_s", "num_d","num_d", "num_i","num_i", "super_s","super_s", "val_b","val_b") );

    // multi-valued strings
    doStatsTemplated( params("facet","true", "rows","0",  "cat_s","cat_ss", "where_s","where_ss", "num_d","num_d", "num_i","num_i", "super_s","super_ss", "val_b","val_b") );
  }


  public void doStatsTemplated(ModifiableSolrParams p) throws Exception {
    macroExpander = new MacroExpander( p.getMap() );

    String cat_s = m("${cat_s}");
    String where_s = m("${where_s}");
    String num_d = m("${num_d}");
    String num_i = m("${num_i}");
    String val_b = m("${val_b}");
    String super_s = m("${super_s}");

    assertU(add(doc("id", "1", cat_s, "A", where_s, "NY", num_d, "4", num_i, "2",   super_s,"zodiac",     val_b, "true")));
    assertU(add(doc("id", "2", cat_s, "B", where_s, "NJ", num_d, "-9", num_i, "-5", super_s,"superman",   val_b, "false")));
    assertU(add(doc("id", "3")));
    assertU(commit());
    assertU(add(doc("id", "4", cat_s, "A", where_s, "NJ", num_d, "2", num_i, "3",   super_s,"spiderman")));
    assertU(add(doc("id", "5", cat_s, "B", where_s, "NJ", num_d, "11", num_i, "7",  super_s,"batman")));
    assertU(commit());
    assertU(add(doc("id", "6", cat_s, "B", where_s, "NY", num_d, "-5", num_i, "-5", super_s,"hulk")));
    assertU(commit());

    // straight query facets
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{catA:{query:{q:'${cat_s}:A'}},  catA2:{query:{query:'${cat_s}:A'}},  catA3:{query:'${cat_s}:A'}    }"
        )
        , "facets=={ 'count':6, 'catA':{ 'count':2}, 'catA2':{ 'count':2}, 'catA3':{ 'count':2}}"
    );

    // nested query facets
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{ catB:{query:{q:'${cat_s}:B', facet:{nj:{query:'${where_s}:NJ'}, ny:{query:'${where_s}:NY'}} }}}"
        )
        , "facets=={ 'count':6, 'catB':{'count':3, 'nj':{'count':2}, 'ny':{'count':1}}}"
    );

    // nested query facets with stats
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{ catB:{query:{q:'${cat_s}:B', facet:{nj:{query:{q:'${where_s}:NJ'}}, ny:{query:'${where_s}:NY'}} }}}"
        )
        , "facets=={ 'count':6, 'catB':{'count':3, 'nj':{'count':2}, 'ny':{'count':1}}}"
    );


    // field/terms facet
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{c1:{field:'${cat_s}'}, c2:{field:{field:'${cat_s}'}}, c3:{terms:{field:'${cat_s}'}}  }"
        )
        , "facets=={ 'count':6, " +
            "'c1':{ /*'stats':{ 'count':5},*/ 'buckets':[{ 'val':'B', 'count':3}, { 'val':'A', 'count':2}]}, " +
            "'c2':{ /*'stats':{ 'count':5},*/ 'buckets':[{ 'val':'B', 'count':3}, { 'val':'A', 'count':2}]}, " +
            "'c3':{ /*'stats':{ 'count':5},*/ 'buckets':[{ 'val':'B', 'count':3}, { 'val':'A', 'count':2}]}} "
    );

    // test mincount
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{field:'${cat_s}', mincount:3}}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ /*'stats':{ 'count':5},*/ 'buckets':[{ 'val':'B', 'count':3}]} } "
    );

    // test default mincount of 1
    assertJQ(req(p, "q", "id:1"
            , "json.facet", "{f1:{terms:'${cat_s}'}}"
        )
        , "facets=={ 'count':1, " +
            "'f1':{ /*'stats':{ 'count':1},*/ 'buckets':[{ 'val':'A', 'count':1}]} } "
    );

    // test  mincount of 0
    assertJQ(req(p, "q", "id:1"
            , "json.facet", "{f1:{terms:{field:'${cat_s}', mincount:0}}}"
        )
        , "facets=={ 'count':1, " +
            "'f1':{ /*'stats':{ 'count':1},*/ 'buckets':[{ 'val':'A', 'count':1}, { 'val':'B', 'count':0}]} } "
    );

    // test  mincount of 0 with stats
    assertJQ(req(p, "q", "id:1"
            , "json.facet", "{f1:{terms:{field:'${cat_s}', mincount:0, allBuckets:true, facet:{n1:'sum(${num_d})'}  }}}"
        )
        , "facets=={ 'count':1, " +
            "'f1':{ allBuckets:{ 'count':1, n1:4.0}, 'buckets':[{ 'val':'A', 'count':1, n1:4.0}, { 'val':'B', 'count':0, n1:0.0}]} } "
    );

    // test sorting by stat
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{field:'${cat_s}', sort:'n1 desc', facet:{n1:'sum(${num_d})'}  }}" +
                " , f2:{terms:{field:'${cat_s}', sort:'n1 asc', facet:{n1:'sum(${num_d})'}  }} }"
        )
        , "facets=={ 'count':6, " +
            "  f1:{ /*stats:{ n1:3.0},*/ 'buckets':[{ val:'A', n1:6.0 }, { val:'B', n1:-3.0}]}" +
            ", f2:{ /*stats:{ n1:3.0},*/ 'buckets':[{ val:'B', n1:-3.0}, { val:'A', n1:6.0 }]} }"
    );

    // terms facet with nested query facet
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{cat:{terms:{field:'${cat_s}', facet:{nj:{query:'${where_s}:NJ'}}    }   }} }"
        )
        , "facets=={ 'count':6, " +
            "'cat':{ /*'stats':{ 'count':5},*/ 'buckets':[{ 'val':'B', 'count':3, 'nj':{ 'count':2}}, { 'val':'A', 'count':2, 'nj':{ 'count':1}}]} }"
    );

    // test prefix
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{field:${super_s}, prefix:s, mincount:0 }}}"  // even with mincount=0, we should only see buckets with the prefix
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[{val:spiderman, count:1}, {val:superman, count:1}]} } "
    );

    // basic range facet
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5}}}"
        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:1}, {val:0.0,count:2}, {val:5.0,count:0} ] } }"
    );


    // range facet with sub facets and stats
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5,   facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}   }}}"
        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:1,x:-5.0,ny:{count:1}}, {val:0.0,count:2,x:5.0,ny:{count:1}}, {val:5.0,count:0,x:0.0,ny:{count:0}} ] } }"
    );

    // stats at top level
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{ sum1:'sum(${num_d})', sumsq1:'sumsq(${num_d})', avg1:'avg(${num_d})', min1:'min(${num_d})', max1:'max(${num_d})', numwhere:'unique(${where_s})' }"
        )
        , "facets=={ 'count':6, " +
            "sum1:3.0, sumsq1:247.0, avg1:0.5, min1:-9.0, max1:11.0, numwhere:2  }"
    );

    // stats at top level, no matches
    // todo: should we just leave stats with no matches out by default?
    assertJQ(req(p, "q", "id:DOESNOTEXIST"
            , "json.facet", "{ sum1:'sum(${num_d})', sumsq1:'sumsq(${num_d})', avg1:'avg(${num_d})', min1:'min(${num_d})', max1:'max(${num_d})', numwhere:'unique(${where_s})' }"
        )
        , "facets=={count:0, " +
            "sum1:0.0, sumsq1:0.0, avg1:0.0, min1:'NaN', max1:'NaN', numwhere:0  }"
    );


    // TODO:
    // missing bucket
    // numdocs('query') stat (don't make a bucket... just a count)
    // merge multiple params
    // make missing configurable in min, max, etc
    // exclusions
    // zeroes
    // instead of json.facet make it facet?
    // unify multiple facet commands...
  }

  MacroExpander macroExpander;
  private String m(String x) {
    return macroExpander.expand(x);
  }



}

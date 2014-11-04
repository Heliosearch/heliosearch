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
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.macro.MacroExpander;
import org.apache.solr.util.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@LuceneTestCase.SuppressCodecs({"Lucene3x","Lucene40","Lucene41","Lucene42","Appending","Asserting"})
public class TestJsonFacets extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    JSONTestUtil.failRepeatedKeys = true;
    initCore("solrconfig-tlog.xml","schema_latest.xml");
  }

  @AfterClass
  public static void afterTests() throws Exception {
    JSONTestUtil.failRepeatedKeys = false;
  }

  public void testStats() throws Exception {
    assertU(add(doc("id", "1", "cat_s", "A", "where_s", "NY", "num_d", "4", "num_i", "2", "val_b", "true",      "sparse_s","one")));
    assertU(add(doc("id", "2", "cat_s", "B", "where_s", "NJ", "num_d", "-9", "num_i", "-5", "val_b", "false")));
    assertU(add(doc("id", "3")));
    assertU(commit());
    assertU(add(doc("id", "4", "cat_s", "A", "where_s", "NJ", "num_d", "2", "num_i", "3")));
    assertU(add(doc("id", "5", "cat_s", "B", "where_s", "NJ", "num_d", "11", "num_i", "7",                      "sparse_s","two")));
    assertU(commit());
    assertU(add(doc("id", "6", "cat_s", "B", "where_s", "NY", "num_d", "-5", "num_i", "-5")));
    assertU(commit());

    // test multiple json.facet commands
    assertJQ(req("q", "*:*", "rows", "0"
            , "json.facet", "{x:'sum(num_d)'}"
            , "json.facet", "{y:'min(num_d)'}"
        )
        , "facets=={count:6 , x:3.0, y:-9.0 }"
    );


    // test streaming
    assertJQ(req("q", "*:*", "rows", "0"
            , "json.facet", "{   cat:{terms:{field:'cat_s', method:stream }}" +
                              ", cat2:{terms:{field:'cat_s', method:stream, sort:'index asc' }}" + // default sort
                              ", cat3:{terms:{field:'cat_s', method:stream, mincount:3 }}" + // mincount
                              ", cat4:{terms:{field:'cat_s', method:stream, prefix:B }}" + // prefix
                              ", cat5:{terms:{field:'cat_s', method:stream, offset:1 }}" + // offset
                " }"
        )
        , "facets=={count:6 " +
            ", cat :{buckets:[{val:A, count:2},{val:B, count:3}]}" +
            ", cat2:{buckets:[{val:A, count:2},{val:B, count:3}]}" +
            ", cat3:{buckets:[{val:B, count:3}]}" +
            ", cat4:{buckets:[{val:B, count:3}]}" +
            ", cat5:{buckets:[{val:B, count:3}]}" +
            " }"
    );


    // test nested streaming under non-streaming
    assertJQ(req("q", "*:*", "rows", "0"
        , "json.facet", "{   cat:{terms:{field:'cat_s', sort:'index asc', facet:{where:{terms:{field:where_s,method:stream}}}   }}}"
        )
        , "facets=={count:6 " +
        ", cat :{buckets:[{val:A, count:2, where:{buckets:[{val:NJ,count:1},{val:NY,count:1}]}   },{val:B, count:3, where:{buckets:[{val:NJ,count:2},{val:NY,count:1}]}    }]}"
        + "}"
    );

    // test nested streaming under streaming
    assertJQ(req("q", "*:*", "rows", "0"
            , "json.facet", "{   cat:{terms:{field:'cat_s', method:stream, facet:{where:{terms:{field:where_s,method:stream}}}   }}}"
        )
        , "facets=={count:6 " +
            ", cat :{buckets:[{val:A, count:2, where:{buckets:[{val:NJ,count:1},{val:NY,count:1}]}   },{val:B, count:3, where:{buckets:[{val:NJ,count:2},{val:NY,count:1}]}    }]}"
            + "}"
    );

    // test nested streaming with stats under streaming
    assertJQ(req("q", "*:*", "rows", "0"
            , "json.facet", "{   cat:{terms:{field:'cat_s', method:stream, facet:{  where:{terms:{field:where_s,method:stream, facet:{x:'max(num_d)'}     }}}   }}}"
        )
        , "facets=={count:6 " +
            ", cat :{buckets:[{val:A, count:2, where:{buckets:[{val:NJ,count:1,x:2.0},{val:NY,count:1,x:4.0}]}   },{val:B, count:3, where:{buckets:[{val:NJ,count:2,x:11.0},{val:NY,count:1,x:-5.0}]}    }]}"
            + "}"
    );

    // test nested streaming with stats under streaming with stats
    assertJQ(req("q", "*:*", "rows", "0",
            "facet","true"
            , "json.facet", "{   cat:{terms:{field:'cat_s', method:stream, facet:{ y:'min(num_d)',  where:{terms:{field:where_s,method:stream, facet:{x:'max(num_d)'}     }}}   }}}"
        )
        , "facets=={count:6 " +
            ", cat :{buckets:[{val:A, count:2, y:2.0, where:{buckets:[{val:NJ,count:1,x:2.0},{val:NY,count:1,x:4.0}]}   },{val:B, count:3, y:-9.0, where:{buckets:[{val:NJ,count:2,x:11.0},{val:NY,count:1,x:-5.0}]}    }]}"
            + "}"
    );


    assertJQ(req("q", "*:*", "fq","cat_s:A")
        , "response/numFound==2"
    );
  }

  @Test
  public void testStatsTemplated() throws Exception {
    // single valued strings
    doStatsTemplated( params(                "rows","0", "noexist","noexist_s",  "cat_s","cat_s", "where_s","where_s", "num_d","num_d", "num_i","num_i", "super_s","super_s", "val_b","val_b", "sparse_s","sparse_s"    ,"multi_ss","multi_ss") );

    // multi-valued strings
    doStatsTemplated( params("facet","true", "rows","0", "noexist","noexist_ss", "cat_s","cat_ss", "where_s","where_ss", "num_d","num_d", "num_i","num_i", "super_s","super_ss", "val_b","val_b", "sparse_s","sparse_ss", "multi_ss","multi_ss") );
  }


  public void doStatsTemplated(ModifiableSolrParams p) throws Exception {
    macroExpander = new MacroExpander( p.getMap() );

    String cat_s = m("${cat_s}");
    String where_s = m("${where_s}");
    String num_d = m("${num_d}");
    String num_i = m("${num_i}");
    String val_b = m("${val_b}");
    String super_s = m("${super_s}");
    String sparse_s = m("${sparse_s}");
    String multi_ss = m("${multi_ss}");

    assertU(add(doc("id", "1", cat_s, "A", where_s, "NY", num_d, "4", num_i, "2",   super_s,"zodiac",   val_b, "true",  sparse_s,"one")));
    assertU(add(doc("id", "2", cat_s, "B", where_s, "NJ", num_d, "-9", num_i, "-5", super_s,"superman", val_b, "false"                , multi_ss,"a", "multi_ss","b" )));
    assertU(add(doc("id", "3")));
    assertU(commit());
    assertU(add(doc("id", "4", cat_s, "A", where_s, "NJ", num_d, "2", num_i, "3",   super_s,"spiderman"                               , multi_ss, "b")));
    assertU(add(doc("id", "5", cat_s, "B", where_s, "NJ", num_d, "11", num_i, "7",  super_s,"batman"                   ,sparse_s,"two", multi_ss, "a")));
    assertU(commit());
    assertU(add(doc("id", "6", cat_s, "B", where_s, "NY", num_d, "-5", num_i, "-5", super_s,"hulk"                                    , multi_ss, "b", multi_ss, "a" )));
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
            "'c1':{ 'buckets':[{ 'val':'B', 'count':3}, { 'val':'A', 'count':2}]}, " +
            "'c2':{  'buckets':[{ 'val':'B', 'count':3}, { 'val':'A', 'count':2}]}, " +
            "'c3':{  'buckets':[{ 'val':'B', 'count':3}, { 'val':'A', 'count':2}]}} "
    );

    // test mincount
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{field:'${cat_s}', mincount:3}}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{  'buckets':[{ 'val':'B', 'count':3}]} } "
    );

    // test default mincount of 1
    assertJQ(req(p, "q", "id:1"
            , "json.facet", "{f1:{terms:'${cat_s}'}}"
        )
        , "facets=={ 'count':1, " +
            "'f1':{  'buckets':[{ 'val':'A', 'count':1}]} } "
    );

    // test  mincount of 0
    assertJQ(req(p, "q", "id:1"
            , "json.facet", "{f1:{terms:{field:'${cat_s}', mincount:0}}}"
        )
        , "facets=={ 'count':1, " +
            "'f1':{  'buckets':[{ 'val':'A', 'count':1}, { 'val':'B', 'count':0}]} } "
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
            "  f1:{  'buckets':[{ val:'A', count:2, n1:6.0 }, { val:'B', count:3, n1:-3.0}]}" +
            ", f2:{  'buckets':[{ val:'B', count:3, n1:-3.0}, { val:'A', count:2, n1:6.0 }]} }"
    );

    // test sorting by count/index order
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{field:'${cat_s}', sort:'count desc' }  }" +
                "           , f2:{terms:{field:'${cat_s}', sort:'count asc'  }  }" +
                "           , f3:{terms:{field:'${cat_s}', sort:'index asc'  }  }" +
                "           , f4:{terms:{field:'${cat_s}', sort:'index desc' }  }" +
                "}"
        )
        , "facets=={ count:6 " +
            " ,f1:{buckets:[ {val:B,count:3}, {val:A,count:2} ] }" +
            " ,f2:{buckets:[ {val:A,count:2}, {val:B,count:3} ] }" +
            " ,f3:{buckets:[ {val:A,count:2}, {val:B,count:3} ] }" +
            " ,f4:{buckets:[ {val:B,count:3}, {val:A,count:2} ] }" +
            "}"
    );


    // terms facet with nested query facet
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{cat:{terms:{field:'${cat_s}', facet:{nj:{query:'${where_s}:NJ'}}    }   }} }"
        )
        , "facets=={ 'count':6, " +
            "'cat':{ 'buckets':[{ 'val':'B', 'count':3, 'nj':{ 'count':2}}, { 'val':'A', 'count':2, 'nj':{ 'count':1}}]} }"
    );

    // test prefix
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{field:${super_s}, prefix:s, mincount:0 }}}"  // even with mincount=0, we should only see buckets with the prefix
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[{val:spiderman, count:1}, {val:superman, count:1}]} } "
    );

    // test prefix that doesn't exist
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{field:${super_s}, prefix:ttt, mincount:0 }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[]} } "
    );

    // test prefix that doesn't exist at start
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{field:${super_s}, prefix:aaaaaa, mincount:0 }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[]} } "
    );

    // test prefix that doesn't exist at end
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{field:${super_s}, prefix:zzzzzz, mincount:0 }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[]} } "
    );

    //
    // missing
    //

    // test missing w/ non-existent field
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{field:${noexist}, missing:true}}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[], missing:{count:6} } } "
    );

    // test missing
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{field:${sparse_s}, missing:true }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[{val:one, count:1}, {val:two, count:1}], missing:{count:4} } } "
    );

    // test missing with stats
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{field:${sparse_s}, missing:true, facet:{x:'sum(num_d)'}   }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[{val:one, count:1, x:4.0}, {val:two, count:1, x:11.0}], missing:{count:4, x:-12.0}   } } "
    );

    // test that the missing bucket is not affected by any prefix
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{field:${sparse_s}, missing:true, prefix:on, facet:{x:'sum(num_d)'}   }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[{val:one, count:1, x:4.0}], missing:{count:4, x:-12.0}   } } "
    );

    // test missing with prefix that doesn't exist
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{field:${sparse_s}, missing:true, prefix:ppp, facet:{x:'sum(num_d)'}   }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[], missing:{count:4, x:-12.0}   } } "
    );

    // test numBuckets
    assertJQ(req(p, "q", "*:*", "rows","0", "facet","true"
            , "json.facet", "{f1:{terms:{field:${cat_s}, numBuckets:true, limit:1}}}" // TODO: limit:0 produced an error
        )
        , "facets=={ 'count':6, " +
            "'f1':{ numBuckets:2, buckets:[{val:B, count:3}]} } "
    );

    // prefix should lower numBuckets
    assertJQ(req(p, "q", "*:*", "rows","0", "facet","true"
            , "json.facet", "{f1:{terms:{field:${cat_s}, numBuckets:true, prefix:B}}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ numBuckets:1, buckets:[{val:B, count:3}]} } "
    );

    // mincount should lower numBuckets
    assertJQ(req(p, "q", "*:*", "rows","0", "facet","true"
            , "json.facet", "{f1:{terms:{field:${cat_s}, numBuckets:true, mincount:3}}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ numBuckets:1, buckets:[{val:B, count:3}]} } "
    );



    // basic range facet
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5}}}"
        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:1}, {val:0.0,count:2}, {val:5.0,count:0} ] } }"
    );

    // basic range facet with "include" params
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5, include:upper}}}"
        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:0}, {val:0.0,count:2}, {val:5.0,count:0} ] } }"
    );

    // range facet with sub facets and stats
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5,   facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}   }}}"
        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:1,x:-5.0,ny:{count:1}}, {val:0.0,count:2,x:5.0,ny:{count:1}}, {val:5.0,count:0,x:0.0,ny:{count:0}} ] } }"
    );

    // range facet with sub facets and stats, with "other:all"
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5, other:all,   facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}   }}}"
        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:1,x:-5.0,ny:{count:1}}, {val:0.0,count:2,x:5.0,ny:{count:1}}, {val:5.0,count:0,x:0.0,ny:{count:0}} ]" +
        ",before: {count:1,x:-5.0,ny:{count:0}}" +
        ",after:  {count:1,x:7.0, ny:{count:0}}" +
        ",between:{count:3,x:0.0, ny:{count:2}}" +
        " } }"
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

    //
    // tests on a multi-valued field with actual multiple values, just to ensure that we are
    // using a multi-valued method for the rest of the tests when appropriate.
    //

    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{cat:{terms:{field:'${multi_ss}', facet:{nj:{query:'${where_s}:NJ'}}    }   }} }"
        )
        , "facets=={ 'count':6, " +
            "'cat':{ 'buckets':[{ 'val':'a', 'count':3, 'nj':{ 'count':2}}, { 'val':'b', 'count':3, 'nj':{ 'count':2}}]} }"
    );

    // test unique on multi-valued field
    assertJQ(req(p, "q", "*:*"
            , "json.facet", "{x:'unique(${multi_ss})', y:{query:{q:'id:2', facet:{x:'unique(${multi_ss})'} }}   }"
        )
        , "facets=={ 'count':6, " +
            "x:2," +
            "y:{count:1, x:2}" +  // single document should yield 2 unique values
            " }"
    );



    // TODO:
    // missing bucket
    // numdocs('query') stat (don't make a bucket... just a count)
    // missing(field)
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

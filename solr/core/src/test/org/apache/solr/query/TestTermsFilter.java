package org.apache.solr.query;


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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.HS;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.theories.suppliers.TestedOn;

import java.util.Random;

public class TestTermsFilter extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "false");
    initCore("solrconfig.xml","schema_latest.xml");
  }

  @Test
  public void testTermsFilter() throws Exception {
    doIndex("foo_s", false);

    // test some prefix compression
    assertJQ(req("q","*:*", "fq","{!terms f=val_s}AAAAAAAAAA1,AAAAAAAAAA3,AAAAAAAAAA2,AAAAAAAAAA4")
        ,"/response/numFound==3"
    );

    // test big request (to test paging, etc)
    StringBuilder sb = new StringBuilder(100000);
    sb.append("{!terms f=val_s}");
    for (int i=0; i<10000; i++) {
      sb.append("" + i + "_some_stuff_here,");
    }
    sb.append("AAAAAAAAAA1,AAAAAAAAAA2,AAAAAAAAAA3");

    assertJQ(req("q","*:*", "fq", sb.toString())
        ,"/response/numFound==3"
    );

    doQuery("id", "1" ,"/response/numFound==1");
    doQuery("id", "2" ,"/response/numFound==1");
    doQuery("id", "3,2,1" ,"/response/numFound==3");
    doQuery("id", "3,999,2,noway,1,what" ,"/response/numFound==3");
    doQuery("id", "" ,"/response/numFound==0");

   // doTerms("id");
    doTerms("foo_s");

    for (String f : new String[] {"foo_s","foo_i","foo_f"}) {
      doIndex(f, false);
      doTerms(f);
      assertU(optimize());
      doTerms(f);
    }

  }


  public void doQuery(String field, String terms, String... tests) throws Exception {
    // turn on debugging to indirectly test toString
    assertJQ(req("q","*:*", "debug","all", "fq","{!terms f=" + field + "}" + terms)
        , tests
    );

    assertJQ(req("q","*:*", "debug","all","fq","{!terms f=" + field + " cache=false}" + terms)
        , tests
    );

    //
    // test as main query instead of filter
    //
    assertJQ(req("q","{!terms f=" + field + "}" + terms, "debug","all")
        , tests
    );

    assertJQ(req("q","{!terms f=" + field + " cache=false}" + terms, "debug","all")
        , tests
    );


    assertJQ(req("q","*:*", "fq","{!terms f=" + field + " sort=false}" + terms, "debug","all")
        , tests
    );
  }

  public void doIndex(String field, boolean optimize) {
    clearIndex();
    assertU(adoc("id", "4", field, "3"));
    assertU(adoc("id", "1", field, "1", "val_s", "AAAAAAAAAA1"));
    assertU(adoc("id", "3", field, "1", "val_s", "AAAAAAAAAA3"));
    assertU(commit());
    assertU(adoc("id", "2", field, "1"));  // deleted
    assertU(adoc("id", "2", field, "2", "val_s", "AAAAAAAAAA2"));
    assertU(commit());
    assertU(adoc("id","5", field,"1"));
    assertU(adoc("id","6", field,"4"));
    assertU(adoc("id","7", field,"2"));
    assertU(adoc("id","8", field,"3"));

    if (optimize) {
      assertU(optimize());
    } else {
      assertU(commit());
    }
  }

  public void doTerms(String field) throws Exception {
    doQuery(field, "1" ,"/response/numFound==3");
    doQuery(field, "2" ,"/response/numFound==2");
    doQuery(field, "3" ,"/response/numFound==2");
    doQuery(field, "4" ,"/response/numFound==1");
    doQuery(field, "5" ,"/response/numFound==0");
    doQuery(field, "" ,"/response/numFound==0");
    doQuery(field, "3,2,1" ,"/response/numFound==7");
  }

  @Test
  public void testBig() throws Exception {
    clearIndex();

    Random r = random();
    int max = r.nextInt(10)+1;  // anywhere from 1 to 10 terms
    int nDocs = 0;

    int docsToIndex=(HS.BUFFER_SIZE_BYTES>>2)+5;

    for (int i=0; i<docsToIndex*2; i++) {
      assertU(adoc("id", "0"+i, "foo_s", "" + r.nextInt(max)));
    }
    assertU(commit());
    max = r.nextInt(10)+1;  // anywhere from 1 to 10 terms

    for (int i=0; i<docsToIndex; i++) {
      assertU(adoc("id", "1"+i, "foo_s", "" + r.nextInt(max)));
    }
    assertU(commit());

    int totalDocs=docsToIndex*3;

    doQuery("foo_s", "5,4,3,2,1,0,9,8,7,6", "/response/numFound=="+totalDocs);
  }

  @Test
  public void testBQPromotion() throws Exception {
    long start,end;
    start = TFilter.num_creations;
    assertJQ(req("q","*:*", "fq","id:(1 2 3 4 5)")
        , "/response=="
    );
    end = TFilter.num_creations;
    assertTrue( end > start );

    // test that the same thing is created again
    start = TFilter.num_creations;
    assertJQ(req("q","*:*", "fq","id:1 id:2 id:3 id:4 id:5")
        , "/response=="
    );
    end = TFilter.num_creations;
    assertTrue( end > start );

    // test nested
    start = TFilter.num_creations;
    assertJQ(req("q","*:*", "fq","id:1 id:2 (id:3 OR (id:4 OR id:5) OR id:6) OR id:7")
        , "/response=="
    );
    end = TFilter.num_creations;
    assertTrue( end > start );



    // no promotion
    start = TFilter.num_creations;
    assertJQ(req("q","*:*", "fq","id:1 foo_s:2 id:3 id:4 id:5")
        , "/response=="
    );
    end = TFilter.num_creations;
    assertTrue( end == start );

    // no promotion
    start = TFilter.num_creations;
    assertJQ(req("q","*:*", "fq","id:1 foo_s:2 id:3 id:4 -id:5")
        , "/response=="
    );
    end = TFilter.num_creations;
    assertTrue( end == start );

    // no promotion
    start = TFilter.num_creations;
    assertJQ(req("q","*:*", "fq","id:1 foo_s:2 id:3 id:4 +id:5")
        , "/response=="
    );
    end = TFilter.num_creations;
    assertTrue( end == start );

    // no promotion
    start = TFilter.num_creations;
    assertJQ(req("q","*:*", "fq","id:1 foo_s:2 id:3 id:4 id:[10 TO 100]")
        , "/response=="
    );
    end = TFilter.num_creations;
    assertTrue( end == start );

  }

}

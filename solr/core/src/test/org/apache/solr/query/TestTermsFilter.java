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

import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DelegatingCollector;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestTermsFilter extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "false");
    initCore("solrconfig.xml","schema_latest.xml");
  }


  public void testTerms() throws Exception {
    clearIndex();
    assertU(adoc("id","4", "val_i","1"));
    assertU(adoc("id","1", "val_i","2", "val_s", "AAAAAAAAAA1"));
    assertU(adoc("id","3", "val_i","3", "val_s", "AAAAAAAAAA3"));
    assertU(commit());
    assertU(adoc("id","2", "val_i","5"));  // deleted
    assertU(adoc("id","2", "val_i","4", "val_s", "AAAAAAAAAA2"));
    assertU(commit());

    assertJQ(req("q","*:*", "fq","{!terms f=id}1")
        ,"/response/numFound==1"
    );

    assertJQ(req("q","*:*", "fq","{!terms f=id}2")
        ,"/response/numFound==1"
    );

    assertJQ(req("q","*:*", "fq","{!terms f=id}1,2,3")
        ,"/response/numFound==3"
    );

    assertJQ(req("q","*:*", "fq","{!terms f=id}3,2,1")
        ,"/response/numFound==3"
    );

    // test integers (needs transformation)
    assertJQ(req("q","*:*", "fq","{!terms f=val_i}3,2,1")
        ,"/response/numFound==3"
    );
    // test integers (needs transformation)
    assertJQ(req("q","*:*", "fq","{!terms f=val_i}1,2,3")
        ,"/response/numFound==3"
    );

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


  }

}

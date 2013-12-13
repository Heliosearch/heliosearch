package org.apache.solr;

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

import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.noggit.JSONUtil;
import org.noggit.ObjectBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestFilterCache extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml","schema12.xml");
  }

  @Test
  public void testFilter() throws Exception {
    assertU(add(doc("id", "1","name", "john", "title", "Director", "dept_s","Engineering")));
    assertU(add(doc("id", "2","name", "mark", "title", "VP", "dept_s","Marketing")));
    assertU(add(doc("id", "3","name", "nancy", "title", "MTS", "dept_s","Sales")));
    assertU(add(doc("id", "4","name", "dave", "title", "MTS", "dept_s","Support", "dept_s","Engineering")));
    assertU(add(doc("id", "5","name", "tina", "title", "VP", "dept_s","Engineering")));

    assertU(add(doc("id","10", "dept_id_s", "Engineering", "text","These guys develop stuff")));
    assertU(add(doc("id","11", "dept_id_s", "Marketing", "text","These guys make you look good")));
    assertU(add(doc("id","12", "dept_id_s", "Sales", "text","These guys sell stuff")));
    assertU(add(doc("id","13", "dept_id_s", "Support", "text","These guys help customers")));

    assertU(commit());

    assertJQ(req("q","*:*", "fq","id:(1 2)"));  // small set
    assertJQ(req("q","*:*", "fq","id:[0 TO 99]"));  // big set
    assertJQ(req("q","*:*", "fq","-id:[0 TO 99]"));  // negative hit
    assertJQ(req("q","*:*", "fq","-id:[0 TO 999]"));  // negative miss
    assertJQ(req("q","*:*", "fq","id:[0 TO 999]"));  // positive hit

    assertJQ(req("q","*:*", "fq","id:(1 2)", "fq","id:(2 3)")); // small+small
    assertJQ(req("q","*:*", "fq","id:(1 2)", "fq","id:[* TO *]")); // small+large
    assertJQ(req("q","*:*", "fq","*:* -id:1", "fq","id:[* TO *]")); // large+large

    assertJQ(req("q","*:*", "fq","id:(1 2)", "fq","id:(2 3)", "fq","id:(3 4)")); // three

    assertJQ(req("q","*:*", "fq","id:(1 2)", "facet","true", "facet.field", "title", "facet.method","enum", "facet.missing","true"));


    /***
    // test cache purge
    for (int i=0; i<1200; i++) {
      assertJQ(req("q","*:*", "fq","id:1 id:"+(i+100)));  // test again for cache hit
    }
    ***/

  }

}

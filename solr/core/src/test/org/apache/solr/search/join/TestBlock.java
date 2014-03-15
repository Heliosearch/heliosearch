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

package org.apache.solr.search.join;

import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.noggit.ObjectBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TestBlock extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml","schema15.xml");
  }

  long totalFound;
  long queriesRun;
  long queriesWithResults;

  private void trackResults(Object jsonReponse) {
    queriesRun++;

    Object o =  ((Map<String,Map<String,Object>>)jsonReponse).get("response").get("numFound");
    int found = ((Number)o).intValue();
    totalFound += found;
    if (found>0) {
      queriesWithResults++;
    }
  }

  private void printResults() {
    log.info("QUERY TRACKING RESULTS: queriesRun="+queriesRun + " queriesWithResults="+queriesWithResults + " average num results=" + ((double)totalFound)/queriesRun );
  }

  private void dumpIndex() throws Exception {
    // TODO: stream/page for big indexes
    SolrQueryRequest req = req("wt","json","indent","true", "_trace","DUMPING INDEX", "echoParams","all",
        "q","*:*", "rows","1000000"
    );

    String rsp = h.query(req);
    log.error("INDEX DUMP :" + rsp);
  }

  private void runQueryAsFilter(ModifiableSolrParams params) {
    params.add("fq",params.get("q"));
    params.set("q","*:*");
  }

  ModifiableSolrParams genericParams = params("wt","json","indent","true", "echoParams","all", "rows","1000000", "fl","id");
  public void doCompare(String q1, String q2, ModifiableSolrParams p) throws Exception {

    ModifiableSolrParams params = params("q", q1);
    params.add(genericParams);
    params.add(p);
    ModifiableSolrParams params2 = params("q", q2);
    params2.add(genericParams);
    params2.add(p);

    boolean runAsFilter = random().nextInt(100) < 20;
    if (runAsFilter) runQueryAsFilter(params);
    if (runAsFilter) runQueryAsFilter(params2);

    SolrQueryRequest req = req(params);
    SolrQueryRequest req2 = req(params2);

    String rsp = h.query(req);
    String rsp2 = h.query(req2);

    Object expected = ObjectBuilder.fromJSON(rsp);
    Object got = ObjectBuilder.fromJSON(rsp2);

    String err = JSONTestUtil.matchObj("/response", got, ((Map) expected).get("response"));
    if (err != null) {
      log.error("JOIN MISMATCH: " + err
          + "\n\texpected="+ rsp
          + "\n\tgot     ="+ rsp2
      );

      dumpIndex();

      // re-execute the request... good for putting a breakpoint here for debugging
      String retry = h.query(req2);

      fail(err);
    }

    trackResults(got);
  }

  public void toParent(String childQuery, String parentType, String... moreParams) throws Exception {
    boolean cache = random().nextInt(100) < 20;
    String joinQ = "{!join from="+parentType+"_s to=id "
        + (cache ? "":" cache=false")
        + "}" + childQuery;
    String blockQ = "{!parent which=type_s:"+parentType
        + (cache ? "":" cache=false")
        +"}" + childQuery;

    doCompare(joinQ, blockQ, params(moreParams));
  }

  public void toChildren(String parentQuery, String parentType, String... moreParams) throws Exception {
    boolean cache = random().nextInt(100) < 20;
    String joinQ = "{!join from=id to="+parentType+"_s"
        + (cache ? "":" cache=false")
        +"}" + parentQuery;

    // for block child join, any doc not marked as a parent is treated as a child.
    // we need to mark all other docs (grandparents, etc) as parents...
    StringBuilder sb = new StringBuilder("type_s:(");
    int lev = parentType.charAt(0) - 'a';
    for (int i=0; i<=lev; i++) {
      sb.append(type(i)).append(' ');
    }
    sb.append(")");
    String allParentTypes=sb.toString();

    String blockQ = "{!child of='"+allParentTypes+"'"
        + (cache ? "":" cache=false")
        +"}" + parentQuery;

    doCompare(joinQ, blockQ, params(moreParams));
  }




  @Test
  public void testJoin() throws Exception {
    // a_s points to parents of type "a", b_s points to parents of type "b", etc.
    SolrInputDocument doc = sdoc("id","1", "type_s","a");

    /***
    SolrInputDocument doc2 = sdoc("id","2", "type_s","b", "parent_s","1", "a_s","1"));
    SolrInputDocument doc3 = sdoc("id","3", "type_s","b", "parent_s","1", "a_s","1"));
    SolrInputDocument doc4 = sdoc("id","4", "type_s","c", "parent_s","3", "a_s","1", "b_s","3"));
    SolrInputDocument doc5 = sdoc("id","5", "type_s","c", "parent_s","3", "a_s","1", "b_s","3"));
    SolrInputDocument doc6 = sdoc("id","6", "type_s","b", "parent_s","1", "a_s","1"));
    SolrInputDocument doc7 = sdoc("id","7", "type_s","b", "parent_s","1", "a_s","1"));
    SolrInputDocument doc8 = sdoc("id","8", "type_s","c", "parent_s","7", "a_s","1", "b_s","7"));
    SolrInputDocument doc9 = sdoc("id","9", "type_s","c", "parent_s","7", "a_s","1", "b_s","7"));
    ***/

    doc.addChildDocument(sdoc("id","2", "type_s","b", "parent_s","1", "a_s","1"));
    doc.addChildDocument(sdoc("id","4", "type_s","c", "parent_s","3", "a_s","1", "b_s","3"));
    doc.addChildDocument(sdoc("id","5", "type_s","c", "parent_s","3", "a_s","1", "b_s","3"));
    doc.addChildDocument(sdoc("id","3", "type_s","b", "parent_s","1", "a_s","1"));
    doc.addChildDocument(sdoc("id","6", "type_s","b", "parent_s","1", "a_s","1"));
    doc.addChildDocument(sdoc("id","8", "type_s","c", "parent_s","7", "a_s","1", "b_s","7"));
    doc.addChildDocument(sdoc("id","9", "type_s","c", "parent_s","7", "a_s","1", "b_s","7"));
    doc.addChildDocument(sdoc("id","7", "type_s","b", "parent_s","1", "a_s","1"));

    assertU(adoc(doc));
    assertU(adoc(doc));       // overwrite original
    assertU(commit());


    // for children, the parent filter needs to be any type of parent, else we will think they are a child...
    toChildren("id:1", "a");
    toChildren("id:1", "a");
    toChildren("id:2", "b");
    toChildren("id:3", "b");
    toChildren("id:(2 6 7)", "b");
    toChildren("id:(6)", "b");
    toChildren("id:(7)", "b");
    toChildren("id:(4 8)", "c");  // test leaf (should match no children)
    toChildren("id:(4)", "c");  // test leaf (should match no children)
    toChildren("id:(5)", "c");  // test leaf (should match no children)

    // child("id:(2 6 7)", "a");  // error case - 2 6 and 7 are not of type "a"... will lead to repeated docs collected

    /***
    SolrQueryRequest req = req("wt","json","indent","true", "echoParams","all",
        "q","{!join from=id to=b_s}id:3"
    );

    String rsp = h.query(req);
    System.out.println(rsp);

    req = req("wt","json","indent","true", "echoParams","all",
        "q","{!child of=type_s:b}id:3"
    );

    rsp = h.query(req);
    System.out.println(rsp);
    ***/


  }



  int id;
  int MAX_LEVEL=5;
  int MAX_CHILDREN=4;
  int CHANCE_CHILDREN=30;  // percent chance a document has a child, reduced by 4*current_level

  private String type(int level) {
    return Character.toString((char)('a'+level));
  }
  private String typeField(int level) {
    return type(level)+"_s";
  }


  private boolean mandateChildren=false;
  private SolrInputDocument randDoc(List<String> parents) {
    if (parents == null) parents=Collections.EMPTY_LIST;

    int level=parents.size();
    SolrInputDocument sdoc = new SolrInputDocument();

    String typeStr = type(level);
    String idStr = Integer.toString(++id) + typeStr;  // append the type to the id, for easier debugging
    sdoc.addField("id", idStr);
    sdoc.addField("type_s", typeStr);
    sdoc.addField("level_i", level);

    // add a_s:<parent_id>, b_s:<parent_id>, etc.
    for (int i=0; i<parents.size(); i++) {
      sdoc.addField(typeField(i), parents.get(i));
    }

    if ( (level < MAX_LEVEL && random().nextInt(100) < (CHANCE_CHILDREN-(level*4)))
         || (level==0 && mandateChildren)
        ) {  // lower chance for children based on level
      int nChildren = random().nextInt(MAX_CHILDREN)+1;
      List<String> newParents = new ArrayList<String>(parents);
      newParents.add(idStr);
      for (int i=0; i<nChildren; i++) {
        SolrInputDocument child = randDoc(newParents);
        sdoc.addChildDocument(child);
      }
    }

    return sdoc;
  }


  int maxDepth;
  List<SolrInputDocument> allDocs;       // all documents indexed
  List<SolrInputDocument>[] docs;        // documents separated by level
  List<SolrInputDocument>[] childDocs;   // docs at the current level and below, used as a working set and shuffled

  private void populate(SolrInputDocument sdoc, int level) {
    maxDepth = Math.max(maxDepth,level);
    docs[level].add(sdoc);
    allDocs.add(sdoc);

    for (int i=level; i<=MAX_LEVEL; i++) {
      childDocs[level].add(sdoc);
    }

    if (sdoc.hasChildDocuments()) {
      for (SolrInputDocument child : sdoc.getChildDocuments()) {
        populate(child, level+1);
      }
    }
  }

  public void indexBlocks(int nDocs) {
    id = 0;
    maxDepth = 0;
    allDocs = new ArrayList<SolrInputDocument>();
    docs = (List<SolrInputDocument>[]) new List[MAX_LEVEL+1];
    childDocs = (List<SolrInputDocument>[]) new List[MAX_LEVEL+1];
    for (int i=0; i<docs.length; i++) {
      docs[i]=new ArrayList<SolrInputDocument>();
      childDocs[i]=new ArrayList<SolrInputDocument>();
    }

    clearIndex();
    while (--nDocs >= 0) {
      SolrInputDocument sdoc = randDoc(null);
      assertU(adoc(sdoc));
      if (random().nextInt(100) < 5) {
        assertU(commit());
      }
      if (random().nextInt(100) < 10) {
        // overwrite the previous doc
        assertU(adoc(sdoc));
      }

      populate(sdoc, 0);
    }
    assertU(commit());
  }

  String randomIds(List<SolrInputDocument> lst, int nDocs) {
    Random r = random();
    Collections.shuffle(lst, r);
    if (nDocs <= 0) {
      nDocs = r.nextInt(lst.size());
      nDocs = r.nextInt(nDocs + 1);   // do random twice to cut down the average size
      nDocs++;
    }

    StringBuilder sb = new StringBuilder("id:(");
    for (int i=0; i<nDocs; i++) {
      sb.append(lst.get(i).getFieldValue("id"));
      sb.append(' ');
    }
    sb.append(")");

    String mainQ=sb.toString();
    return mainQ;
  }


  @Test
  public void testRandomJoin() throws Exception {
    int indexIter=25;
    int queryIter=100;

    for (int iiter=0; iiter<indexIter; iiter++) {
      int topLevelDocs=random().nextInt(30)+1;
      indexBlocks(topLevelDocs);
      if (maxDepth<=1) continue;  // need more than one level to test

      for(int qiter=0; qiter<queryIter; qiter++) {
        // use filters to exercise skipping on the scorers too
        String filter = null;
        if (random().nextInt(100) < 70) {  // filter 70 percent of the queries
          int filterSize;

          // we normally want filters that match a majority of the documents for
          // better coverage (else results will often match because everything is filtered out.
          if (random().nextInt(100)<70) {
            filterSize = (int)(allDocs.size()*.9);  // match 90 percent of the documents
          } else {
            filterSize = random().nextInt(allDocs.size());
          }

          filterSize = Math.max(1, filterSize);

          filter = randomIds(allDocs, filterSize);

          if (random().nextInt(100) < 50) {
            filter = "{!cache=false}"+filter;
          }
        }

        // query some children and match to their parents.
        // min level is 1 since we're not supposed to match a parent with the query
        int childLevel = random().nextInt(maxDepth-1)+1;

        // we can map to any parent level above the child level
        int parentLevel = random().nextInt(childLevel);

        // collect some ids of type childLevel or below
        String mainQ = randomIds(childDocs[childLevel], 0);

        if (filter == null) {
          toParent(mainQ, type(parentLevel));
        } else {
          toParent(mainQ, type(parentLevel), "fq", filter);
        }

        parentLevel = random().nextInt(maxDepth);

        // For toChildren, the join query must only hit parent types (defined by the parent filter)
        mainQ = randomIds(docs[parentLevel], 0);

        if (filter == null) {
          toChildren(mainQ, type(parentLevel));
        } else {
          toChildren(mainQ, type(parentLevel), "fq", filter);
        }

      }

    }

    printResults();
  }


}

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
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.noggit.JSONUtil;
import org.noggit.ObjectBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SolrTestCaseHS extends SolrTestCaseJ4 {

  @SafeVarargs
  public static <T> Set<T> set(T... a) {
    LinkedHashSet<T> s = new LinkedHashSet<>();
    for (T t : a) {
      s.add(t);
    }
    return s;
  }

  public static Map<String,Object> toObject(Doc doc, IndexSchema schema, Collection<String> fieldNames) {
    Map<String,Object> result = new HashMap<>();
    for (Fld fld : doc.fields) {
      if (fieldNames != null && !fieldNames.contains(fld.ftype.fname)) continue;
      SchemaField sf = schema.getField(fld.ftype.fname);
      if (!sf.multiValued()) {
        result.put(fld.ftype.fname, fld.vals.get(0));
      } else {
        result.put(fld.ftype.fname, fld.vals);
      }
    }
    return result;
  }
  
  
  public static Object createDocObjects(Map<Comparable, Doc> fullModel, Comparator sort, int rows, Collection<String> fieldNames) {
    List<Doc> docList = new ArrayList<Doc>(fullModel.values());
    Collections.sort(docList, sort);
    List sortedDocs = new ArrayList(rows);
    for (Doc doc : docList) {
      if (sortedDocs.size() >= rows) break;
      Map<String,Object> odoc = toObject(doc, h.getCore().getLatestSchema(), fieldNames);
      sortedDocs.add(toObject(doc, h.getCore().getLatestSchema(), fieldNames));
    }
    return sortedDocs;
  }


  public static void compare(SolrQueryRequest req, String path, Object model, Map<Comparable, Doc> fullModel) throws Exception {
    String strResponse = h.query(req);

    Object realResponse = ObjectBuilder.fromJSON(strResponse);
    String err = JSONTestUtil.matchObj(path, realResponse, model);
    if (err != null) {
      log.error("RESPONSE MISMATCH: " + err
              + "\n\trequest="+req
              + "\n\tresult="+strResponse
              + "\n\texpected="+ JSONUtil.toJSON(model)
              + "\n\tmodel="+ fullModel
      );

      // re-execute the request... good for putting a breakpoint here for debugging
      String rsp = h.query(req);

      fail(err);
    }

  }

  public static void clearNCache() {
    SolrQueryRequest req = req();
    req.getSearcher().getnCache().clear();
    req.close();
  }

  public static void clearQueryCache() {
    SolrQueryRequest req = req();
    req.getSearcher();
    req.close();
  }


}

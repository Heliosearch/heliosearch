package org.apache.solr.request.macro;

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


import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RequestUtil {
  /**
   * Set default-ish params on a SolrQueryRequest as well as do standard macro processing.
   *
   *
   * @param req The request whose params we are interested in
   * @param defaults values to be used if no values are specified in the request params
   * @param appends values to be appended to those from the request (or defaults) when dealing with multi-val params, or treated as another layer of defaults for singl-val params.
   * @param invariants values which will be used instead of any request, or default values, regardless of context.
   */
  public static void processParams(SolrQueryRequest req, SolrParams defaults,
                                 SolrParams appends, SolrParams invariants) {

    SolrParams p = req.getParams();

    // short circuit processing
    if (defaults == null && invariants == null && appends == null && !p.getBool("expandMacros", true)) {
      return;  // nothing to do...
    }

    Map<String, String[]> newMap = asMultiMap(p, true);

    if (defaults != null) {
      Map<String, String[]> defaultsMap = asMultiMap(defaults);
      for (Map.Entry<String, String[]> entry : defaultsMap.entrySet()) {
        String key = entry.getKey();
        if (!newMap.containsKey(key)) {
          newMap.put(key, entry.getValue());
        }
      }
    }

    if (appends != null) {
      Map<String, String[]> appendsMap = asMultiMap(appends);

      for (Map.Entry<String, String[]> entry : appendsMap.entrySet()) {
        String key = entry.getKey();
        String[] arr = newMap.get(key);
        if (arr == null) {
          newMap.put(key, entry.getValue());
        } else {
          String[] appendArr = entry.getValue();
          String[] newArr = new String[arr.length + appendArr.length];
          System.arraycopy(arr, 0, newArr, 0, arr.length);
          System.arraycopy(appendArr, 0, newArr, arr.length, appendArr.length);
          newMap.put(key, newArr);
        }
      }
    }


    // first populate defaults, etc..
    if (invariants != null) {
      newMap.putAll( asMultiMap(invariants) );
    }

    String[] doMacrosStr = newMap.get("expandMacros");
    boolean doMacros = true;
    if (doMacrosStr != null) {
      doMacros = "true".equals(doMacrosStr[0]);
    }

    SolrParams newParams;
    if (doMacros) {
      Map<String, String[]> expandedMap = MacroExpander.expand(newMap);
      newParams = new MultiMapSolrParams(expandedMap);
    } else {
      newParams = new MultiMapSolrParams(newMap);
    }

    req.setParams(newParams);
  }

  public static Map<String,String[]> asMultiMap(SolrParams params) {
    return asMultiMap(params, false);
  }

  public static Map<String,String[]> asMultiMap(SolrParams params, boolean newCopy) {
    if (params == null) {
      if (newCopy) {
        return new HashMap<>();
      }
      return Collections.emptyMap();
    } else if (params instanceof MultiMapSolrParams) {
      Map<String,String[]> map = ((MultiMapSolrParams)params).getMap();
      if (newCopy) {
        return new HashMap<>(map);
      }
      return map;
    } else {
      Map<String,String[]> map = new HashMap<>();
      Iterator<String> iterator = params.getParameterNamesIterator();
      while (iterator.hasNext()) {
        String name = iterator.next();
        map.put(name, params.getParams(name));
      }
      return map;
    }
  }
}

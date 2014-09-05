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

import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Callback;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrConstantScoreQuery;
import org.apache.solr.util.CharUtils;

import java.util.ArrayList;
import java.util.Collections;


public class TermsQParserPlugin extends QParserPlugin {
  public static final String NAME = "terms";
  private static String SEPARATOR = "separator";
  private static String SORT = "sort";


   @Override
   public void init(NamedList args) {
   }

   @Override
   public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
     return new QParser(qstr, localParams, params, req) {
       @Override
       public Query parse() {
         final String field = localParams.get(QueryParsing.F);
         final String termStr = localParams.get(QueryParsing.V);
         final boolean sort = localParams.getBool(SORT, true);
         final String separator = localParams.get(SEPARATOR, ",");
         if (separator.length() > 1) {
           throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "separator must be single character, got '" + separator + "'");
         }
         char sepChar = separator.charAt(0);


         if (sort) {
           final ArrayList<BytesRef> lst = new ArrayList<>(termStr.length() >> 3);
           final int[] nbytes = new int[1];

           CharUtils.splitSmart(termStr, sepChar, true, new Callback<CharSequence>() {
             BytesRef br = new BytesRef();
             SchemaField sf = req.getSchema().getField(field);

             @Override
             public long callback(CharSequence info) {
               sf.getType().readableToIndexed(info, br);
               nbytes[0] += br.length;
               lst.add(br);
               br = new BytesRef(br.length + 1);  // use last length as guide
               return 0;
             }
           });


           Collections.sort(lst);

           TFilter.Builder builder = new TFilter.Builder(field, termStr.length());
           try {
             BytesRef prev = new BytesRef();
             for (BytesRef br : lst) {
               builder.addTerm(prev, br);
               prev = br;
             }
             return builder.buildQuery();
           } finally {
             builder.close();
           }
         }


         // if not sorting, build incrementally to avoid instantiating entire list
         try (TFilter.Builder builder = new TFilter.Builder(field, termStr.length())) {

           CharUtils.splitSmart(termStr, sepChar, true, new Callback<CharSequence>() {
             BytesRef br = new BytesRef();
             BytesRef prev = new BytesRef();
             final SchemaField sf = req.getSchema().getField(field);

             @Override
             public long callback(CharSequence info) {
               BytesRef tmp = prev;
               prev = br;
               br = tmp;
               br.offset = br.length = 0;
               sf.getType().readableToIndexed(info, br);
               builder.addTerm(prev, br);
               return 0;
             }
           });

           return builder.buildQuery();
         }

       }


     };
   }
 }

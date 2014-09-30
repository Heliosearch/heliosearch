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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.FunctionQParserPlugin;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.function.FuncValues;
import org.apache.solr.search.function.FunctionQuery;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.valuesource.MaxFloatFunction;
import org.apache.solr.search.function.valuesource.MinFloatFunction;
import org.apache.solr.search.mutable.MutableValueInt;

import java.io.Closeable;
import java.io.IOException;

abstract class Acc extends Collector implements Closeable {
  String key;  // TODO

  public void finish() {
  }

  public abstract Comparable getValue();

  public void setValues(NamedList<Object> bucket) {
    if (key == null) return;
    bucket.add(key, getValue());
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
  }

  @Override
  public void collect(int doc) throws IOException {
  }

  @Override
  public void setNextReader(AtomicReaderContext readerContext) throws IOException {
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }

  @Override
  public void close() throws IOException {
  }
}




/****
// model whole group as an as Acc?
class AccGroup {
  List<AccInfo> accInfos;
  SlotAcc[] accs;

  // how to deal with an Acc that can produce multiple statistics?
  // an avg accumulator can also produce counts!

  // how to express Sort?
  // facet.sort=count  facet.sort=index  facet.sort=stat   facet.sort=sum(price)  // allow full facet.stat syntax.... facet.sort=x:sum(price)?

  // TODO: global sums?
  // if you ask for sum(price).... it might be nice to compare it against the entire set!  Same for avg, min, max, etc...
  // all of these can be calculated relatively easily once other stats are gathered, so it definitely seems like
  // we should do this by default!

  public void addStat(String )

  public void setSort(String facetSort) {

  }

}
***/




public class AccInfo {
  String statString;
  String key;  // the output key
  AggValueSource agg;

  // provide as much context as possible here to enable optimization by selecting different accumulators
  public SlotAcc createSlotAcc(MutableValueInt slot, QueryContext qContext, SolrQueryRequest req, SolrIndexSearcher searcher, int numDocs, int numSlots) throws IOException {
    SlotAcc acc = null;

    // this is temporary - longer term, the AggValueSource should be responsible for this...
    if (agg instanceof StrAggValueSource) {
      String arg = ((StrAggValueSource) agg).getArg();
      if ("count".equals(agg.name())) {
        acc = new CountSlotAcc(slot, qContext, numSlots);
      } else if ("unique".equals(agg.name())) {
        String fname = arg;
        SchemaField sf = searcher.getSchema().getField(fname);
        if (sf.multiValued() || sf.getType().multiValuedFieldCache()) {
          acc = new UniqueMultivaluedSlotAcc(slot, qContext, arg, numSlots);
        } else {
          acc = new UniqueSinglevaluedSlotAcc(slot, qContext, arg, numSlots);
        }
      }
    } else if (agg instanceof SimpleAggValueSource) {
      SimpleAggValueSource simple = (SimpleAggValueSource)agg;
      if ("sum".equals(simple.name())) {
        acc = new SumSlotAcc(slot, simple.getArg(), qContext, numSlots);
      } else if ("avg".equals(simple.name())) {
        acc = new AvgSlotAcc(slot, simple.getArg(), qContext, numSlots);
      } else if ("min".equals(simple.name())) {
        acc = new MinSlotAcc(slot, simple.getArg(), qContext, numSlots);
      } else if ("max".equals(simple.name())) {
        acc = new MaxSlotAcc(slot, simple.getArg(), qContext, numSlots);
      } else if ("sumsq".equals(simple.name())) {
        acc = new SumsqSlotAcc(slot, simple.getArg(), qContext, numSlots);
      }

    }

    if (acc == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown facet.stat " + statString);
    }

    acc.key = key;

    return acc;
  }

  // TODO: allow multiple in a single line?  a:sum(x), b:avg(x)?
  public static AccInfo parseFacetStat(String str, SolrQueryRequest req) {
    AccInfo stat = new AccInfo();
    stat.statString = str;

    try {
      String keyAndValue = str;
      String key = null;
      SolrParams localParams = QueryParsing.getLocalParams(str, req.getParams());
      if (localParams != null) {
        keyAndValue = localParams.get(CommonParams.VALUE);
        key = localParams.get(CommonParams.OUTPUT_KEY);
      }

      QueryParsing.StrParser sp = new QueryParsing.StrParser(keyAndValue);
      int start = 0;
      sp.eatws();
      if (sp.pos >= sp.end) return null;

      // try key:func() format
      String funcStr = keyAndValue;

      if (key == null) {
        key = SolrReturnFields.getFieldName(sp);
        if (key != null && sp.opt(':')) {
          // OK, we got the key
          funcStr = keyAndValue.substring(sp.pos);
        } else {
          // an invalid key... it must not be present.
          sp.pos = start;
          key = null;
        }
      }

      if (key == null) {
        key = funcStr;  // not really ideal
      }
      stat.key = key;

      // special case "count" to be equal to "count()" since we have the legacy facet.sort=count
      if ("count".equals(funcStr)) {
        stat.agg = new StrAggValueSource("count", null);
        return stat;
      }

      // let's try it as a function instead
      FunctionQParser parser = (FunctionQParser)QParser.getParser(funcStr, FunctionQParserPlugin.NAME, req);
      AggValueSource agg = parser.parseAgg(FunctionQParser.FLAG_DEFAULT);
      stat.agg = agg;

      return stat;

    } catch (SyntaxError e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error parsing facet.stat " + str, e);
    }
  }

  public static AggValueSource parseStat(String str, SolrQueryRequest req) {
return null;
  }
}



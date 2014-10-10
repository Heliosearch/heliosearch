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


import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.DateField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.SortableDoubleField;
import org.apache.solr.schema.SortableFloatField;
import org.apache.solr.schema.SortableIntField;
import org.apache.solr.schema.SortableLongField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.FunctionQParserPlugin;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.mutable.MutableValueInt;
import org.apache.solr.util.DateMathParser;
import org.noggit.ObjectBuilder;


class FacetModule {
  public static FacetRequest getFacetRequest(SolrQueryRequest req) {
    // String[] jsonFacets = req.getParams().getParams("json.facet");
    // TODO: allow multiple

    String jsonFacet = req.getParams().get("json.facet"); // TODO... allow just "facet" also?
    if (jsonFacet == null) {
      return null;
    }

    Object facetArgs = null;
    try {
      facetArgs = ObjectBuilder.fromJSON(jsonFacet);
    } catch (IOException e) {
      // should be impossible
     // TODO: log
    }

    FacetParser parser = new FacetTopParser(req);
    try {
      FacetRequest facetReq = parser.parse(facetArgs);
      return facetReq;
    } catch (SyntaxError syntaxError) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, syntaxError);
    }
  }


  public static boolean doFacets(ResponseBuilder rb) throws IOException {
    FacetRequest freq = getFacetRequest(rb.req);
    if (freq == null) return false;

    FacetContext fcontext = new FacetContext();
    fcontext.base = rb.getResults().docSet;
    fcontext.req = rb.req;
    fcontext.searcher = rb.req.getSearcher();
    fcontext.qcontext = QueryContext.newContext(fcontext.searcher);


    FacetProcessor fproc = freq.createFacetProcessor(fcontext);
    fproc.process();
    rb.rsp.add("facets", fproc.getResponse());
    return true;
  }

}


abstract class FacetRequest {
  protected Map<String,AggValueSource> facetStats;  // per-bucket statistics
  protected Map<String,FacetRequest> subFacets;     // list of facets
  protected List<String> excludeFilters;

  public FacetRequest() {
    facetStats = new LinkedHashMap<>();
    subFacets = new LinkedHashMap<>();
  }

  public Map<String, AggValueSource> getFacetStats() {
    return facetStats;
  }

  public Map<String, FacetRequest> getSubFacets() {
    return subFacets;
  }

  public void addStat(String key, AggValueSource stat) {
    facetStats.put(key, stat);
  }

  public void addSubFacet(String key, FacetRequest facetRequest) {
    subFacets.put(key, facetRequest);
  }

  public abstract FacetProcessor createFacetProcessor(FacetContext fcontext);
}


class FacetContext {
  // Context info for actually executing a local facet command
  QueryContext qcontext;
  SolrQueryRequest req;  // TODO: or do params?
  SolrIndexSearcher searcher;
  DocSet base;
  FacetContext parent;

  public FacetContext sub() {
    FacetContext ctx = new FacetContext();
    ctx.qcontext = qcontext;
    ctx.req = req;
    ctx.searcher = searcher;
    ctx.base = base;

    ctx.parent = this;
    return ctx;
  }
}


class FacetProcessor<FacetRequestT extends FacetRequest>  {
  protected SimpleOrderedMap<Object> response;
  protected FacetContext fcontext;
  protected FacetRequestT freq;

  LinkedHashMap<String,SlotAcc> accMap;
  protected SlotAcc[] accs;
  protected SlotAcc countAcc;
  protected MutableValueInt slot;

  FacetProcessor(FacetContext fcontext, FacetRequestT freq) {
    this.fcontext = fcontext;
    this.freq = freq;
  }

  public void process() throws IOException {


  }

  public Object getResponse() {
    return null;
  }


  protected void createAccs(int docCount, int slotCount) throws IOException {
    accMap = new LinkedHashMap<String,SlotAcc>();
    slot = new MutableValueInt();
    for (Map.Entry<String,AggValueSource> entry : freq.getFacetStats().entrySet()) {
      SlotAcc acc = entry.getValue().createSlotAcc(fcontext, slot, docCount, slotCount);
      acc.key = entry.getKey();
      accMap.put(acc.key, acc);

      if (acc instanceof CountSlotAcc) {
        countAcc = acc;
      }
    }
  }

  /** Create the actual accs array from accMap before starting to collect stats. */
  protected void prepareForCollection() {
    accs = new SlotAcc[accMap.size()];
    int i=0;
    if (countAcc != null) {
      // put countAcc first in case others depend on it...
      accs[i++] = countAcc;
    }
    for (SlotAcc acc : accMap.values()) {
      if (acc == countAcc) {
        continue;
      }
      accs[i++] = acc;
    }
  }

  protected void processStats(NamedList<Object> bucket, DocSet docs, int docCount) throws IOException {
    if (freq.getFacetStats().size() == 0) return;
    createAccs(docCount, 1);
    prepareForCollection();
    collect(docs);
    addStats(bucket, 0);
  }


  protected void fillBucketSubs(NamedList<Object> response, FacetContext subContext) throws IOException {
    for (Map.Entry<String,FacetRequest> sub : freq.getSubFacets().entrySet()) {
      FacetProcessor subProcessor = sub.getValue().createFacetProcessor(subContext);
      subProcessor.process();
      response.add( sub.getKey(), subProcessor.getResponse() );
    }
  }


  protected int collect(int slotNum, DocSet docs) throws IOException {
    slot.value = slotNum;
    return collect(docs);
  }

  protected int collect(DocSet docs) throws IOException {
    int count = 0;
    SolrIndexSearcher searcher = fcontext.searcher;

    final List<AtomicReaderContext> leaves = searcher.getIndexReader().leaves();
    final Iterator<AtomicReaderContext> ctxIt = leaves.iterator();
    AtomicReaderContext ctx = null;
    int segBase = 0;
    int segMax;
    int adjustedMax = 0;
    for (DocIterator docsIt = docs.iterator(); docsIt.hasNext(); ) {
      final int doc = docsIt.nextDoc();
      if (doc >= adjustedMax) {
        do {
          ctx = ctxIt.next();
          if (ctx == null) {
            // should be impossible
            throw new RuntimeException("INTERNAL FACET ERROR");
          }
          segBase = ctx.docBase;
          segMax = ctx.reader().maxDoc();
          adjustedMax = segBase + segMax;
        } while (doc >= adjustedMax);
        assert doc >= ctx.docBase;
        setNextReader(ctx);
      }
      count++;
      collect(doc - segBase);  // per-seg collectors
    }
    return count;
  }

  void collect(int tnum, int segDoc) throws IOException {
    slot.value = tnum;
    collect(segDoc);
  }

  void collect(int segDoc) throws IOException {
    for (SlotAcc acc : accs) {
      acc.collect(segDoc);
    }
  }

  void setNextReader(AtomicReaderContext ctx) throws IOException {
    for (SlotAcc acc : accs) {
      acc.setNextReader(ctx);
    }
  }

  void addStats(NamedList<Object> target, int slotNum) {
    slot.value = slotNum;
    for (Acc acc : accs) {
      acc.setValues(target);
    }
  }





  public void fillBucket(SimpleOrderedMap<Object> bucket, Query q) throws IOException {
    boolean needDocSet = freq.getFacetStats().size() > 0 || freq.getSubFacets().size() > 0;

    // TODO: always collect counts or not???

    DocSet result = null;
    int count;

    if (needDocSet) {
      if (q == null) {
        result = fcontext.base;
        result.incref();
      } else {
        result = fcontext.searcher.getDocSet(q);
      }
      count = result.size();
    } else {
      if (q == null) {
        count = fcontext.base.size();
      } else {
        count = fcontext.searcher.numDocs(q, fcontext.base);
      }
    }

    try {
      bucket.add("count", count);
      processStats(bucket, result, (int)count);
      processSubs(bucket, result);
    } finally {
      if (result != null) {
        result.decref();
        result = null;
      }
    }
  }




  private void processSubs(NamedList<Object> bucket, DocSet result) throws IOException {
    // TODO: process exclusions, etc

    FacetContext subContext = fcontext.sub();
    subContext.base = result;

    fillBucketSubs(bucket, subContext);
  }


}





class FacetQueryProcessor extends FacetProcessor<FacetQuery> {
  FacetQueryProcessor(FacetContext fcontext, FacetQuery freq) {
    super(fcontext, freq);
  }

  @Override
  public Object getResponse() {
    return response;
  }

  @Override
  public void process() throws IOException {
    response = new SimpleOrderedMap<>();
    fillBucket(response, freq.q);
  }


}




class FacetRangeProcessor extends FacetProcessor<FacetRange> {
  SchemaField sf;


  FacetRangeProcessor(FacetContext fcontext, FacetRange freq) {
    super(fcontext, freq);
  }

  @Override
  public void process() throws IOException {
    sf = fcontext.searcher.getSchema().getField(freq.field);

    response = getRangeCountsIndexed();
  }

  @Override
  public Object getResponse() {
    return response;
  }


  SimpleOrderedMap<Object> getRangeCountsIndexed() throws IOException {
    final FieldType ft = sf.getType();

    RangeEndpointCalculator<?> calc = null;

    if (ft instanceof TrieField) {
      final TrieField trie = (TrieField)ft;

      switch (trie.getType()) {
        case FLOAT:
          calc = new FloatRangeEndpointCalculator(sf);
          break;
        case DOUBLE:
          calc = new DoubleRangeEndpointCalculator(sf);
          break;
        case INTEGER:
          calc = new IntegerRangeEndpointCalculator(sf);
          break;
        case LONG:
          calc = new LongRangeEndpointCalculator(sf);
          break;
        default:
          throw new SolrException
              (SolrException.ErrorCode.BAD_REQUEST,
                  "Unable to range facet on tried field of unexpected type:" + freq.field);
      }
    } else if (ft instanceof DateField) {
      calc = new DateRangeEndpointCalculator(sf, null);
    } else if (ft instanceof SortableIntField) {
      calc = new IntegerRangeEndpointCalculator(sf);
    } else if (ft instanceof SortableLongField) {
      calc = new LongRangeEndpointCalculator(sf);
    } else if (ft instanceof SortableFloatField) {
      calc = new FloatRangeEndpointCalculator(sf);
    } else if (ft instanceof SortableDoubleField) {
      calc = new DoubleRangeEndpointCalculator(sf);
    } else {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "Unable to range facet on field:" + sf);
    }

    return getRangeCountsIndexed(calc);
  }

  private <T extends Comparable<T>> SimpleOrderedMap getRangeCountsIndexed(RangeEndpointCalculator<T> calc) throws IOException {

    final SimpleOrderedMap<Object> res = new SimpleOrderedMap<>();

    List<SimpleOrderedMap<Object>> buckets = null;
    NamedList<Integer> counts = null;

    buckets = new ArrayList<>();
    res.add("buckets", buckets);

    T start = calc.getValue(freq.start.toString());
    T end = calc.getValue(freq.end.toString());
    EnumSet<FacetParams.FacetRangeInclude> include = freq.include;

    /***
    if (end.compareTo(start) < 0) {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "range facet 'end' comes before 'start': "+end+" < "+start);
    }
    ***/

    String gap = freq.gap.toString();

    final int minCount = 0;

    T low = start;

    while (low.compareTo(end) < 0) {
      T high = calc.addGap(low, gap);
      if (end.compareTo(high) < 0) {
        if (freq.hardend) {
          high = end;
        } else {
          end = high;
        }
      }
      if (high.compareTo(low) < 0) {
        throw new SolrException
            (SolrException.ErrorCode.BAD_REQUEST,
                "range facet infinite loop (is gap negative? did the math overflow?)");
      }
      if (high.compareTo(low) == 0) {
        throw new SolrException
            (SolrException.ErrorCode.BAD_REQUEST,
                "range facet infinite loop: gap is either zero, or too small relative start/end and caused underflow: " + low + " + " + gap + " = " + high );
      }

      final boolean includeLower =
          (include.contains(FacetParams.FacetRangeInclude.LOWER) ||
              (include.contains(FacetParams.FacetRangeInclude.EDGE) &&
                  0 == low.compareTo(start)));
      final boolean includeUpper =
          (include.contains(FacetParams.FacetRangeInclude.UPPER) ||
              (include.contains(FacetParams.FacetRangeInclude.EDGE) &&
                  0 == high.compareTo(end)));

      final String lowS = calc.formatValue(low);
      final String highS = calc.formatValue(high);

      Object label = low;
      buckets.add( rangeStats(low, minCount,lowS, highS, includeLower, includeUpper) );

      low = high;
    }

      // no matter what other values are listed, we don't do
      // anything if "none" is specified.
      if (! freq.others.contains(FacetParams.FacetRangeOther.NONE) ) {

        boolean all = freq.others.contains(FacetParams.FacetRangeOther.ALL);
        final String startS = calc.formatValue(start);
        final String endS = calc.formatValue(end);

        if (all || freq.others.contains(FacetParams.FacetRangeOther.BEFORE)) {
          // include upper bound if "outer" or if first gap doesn't already include it
          res.add(FacetParams.FacetRangeOther.BEFORE.toString(),
              rangeStats(null, 0, null, startS,
                  false,
                  (include.contains(FacetParams.FacetRangeInclude.OUTER) ||
                      (!(include.contains(FacetParams.FacetRangeInclude.LOWER) ||
                          include.contains(FacetParams.FacetRangeInclude.EDGE))))));

        }
        if (all || freq.others.contains(FacetParams.FacetRangeOther.AFTER)) {
          // include lower bound if "outer" or if last gap doesn't already include it
          res.add(FacetParams.FacetRangeOther.AFTER.toString(),
              rangeStats(null, 0, endS, null,
                  (include.contains(FacetParams.FacetRangeInclude.OUTER) ||
                      (!(include.contains(FacetParams.FacetRangeInclude.UPPER) ||
                          include.contains(FacetParams.FacetRangeInclude.EDGE)))),
                  false));
        }
        if (all || freq.others.contains(FacetParams.FacetRangeOther.BETWEEN)) {
          res.add(FacetParams.FacetRangeOther.BETWEEN.toString(),
              rangeStats(null, 0, startS, endS,
                  (include.contains(FacetParams.FacetRangeInclude.LOWER) ||
                      include.contains(FacetParams.FacetRangeInclude.EDGE)),
                  (include.contains(FacetParams.FacetRangeInclude.UPPER) ||
                      include.contains(FacetParams.FacetRangeInclude.EDGE))));

        }
      }


    return res;
  }

  private SimpleOrderedMap<Object> rangeStats(Object label, int mincount, String low, String high, boolean iLow, boolean iHigh) throws IOException {
    SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();

    // typically the start value of the range, but null for before/after/between
    if (label != null) {
      bucket.add("val", label);
    }

    Query rangeQ = sf.getType().getRangeQuery(null, sf, low, high, iLow, iHigh);
    fillBucket(bucket, rangeQ);

    return bucket;
  }




  // Essentially copied from SimpleFacets...
  // would be nice to unify this stuff w/ analytics component...
  /**
   * Perhaps someday instead of having a giant "instanceof" case
   * statement to pick an impl, we can add a "RangeFacetable" marker
   * interface to FieldTypes and they can return instances of these
   * directly from some method -- but until then, keep this locked down
   * and private.
   */
  private static abstract class RangeEndpointCalculator<T extends Comparable<T>> {
    protected final SchemaField field;
    public RangeEndpointCalculator(final SchemaField field) {
      this.field = field;
    }

    /**
     * Formats a Range endpoint for use as a range label name in the response.
     * Default Impl just uses toString()
     */
    public String formatValue(final T val) {
      return val.toString();
    }
    /**
     * Parses a String param into an Range endpoint value throwing
     * a useful exception if not possible
     */
    public final T getValue(final String rawval) {
      try {
        return parseVal(rawval);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Can't parse value "+rawval+" for field: " +
                field.getName(), e);
      }
    }
    /**
     * Parses a String param into an Range endpoint.
     * Can throw a low level format exception as needed.
     */
    protected abstract T parseVal(final String rawval)
        throws java.text.ParseException;

    /**
     * Parses a String param into a value that represents the gap and
     * can be included in the response, throwing
     * a useful exception if not possible.
     *
     * Note: uses Object as the return type instead of T for things like
     * Date where gap is just a DateMathParser string
     */
    public final Object getGap(final String gap) {
      try {
        return parseGap(gap);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Can't parse gap "+gap+" for field: " +
                field.getName(), e);
      }
    }

    /**
     * Parses a String param into a value that represents the gap and
     * can be included in the response.
     * Can throw a low level format exception as needed.
     *
     * Default Impl calls parseVal
     */
    protected Object parseGap(final String rawval)
        throws java.text.ParseException {
      return parseVal(rawval);
    }

    /**
     * Adds the String gap param to a low Range endpoint value to determine
     * the corrisponding high Range endpoint value, throwing
     * a useful exception if not possible.
     */
    public final T addGap(T value, String gap) {
      try {
        return parseAndAddGap(value, gap);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Can't add gap "+gap+" to value " + value +
                " for field: " + field.getName(), e);
      }
    }
    /**
     * Adds the String gap param to a low Range endpoint value to determine
     * the corrisponding high Range endpoint value.
     * Can throw a low level format exception as needed.
     */
    protected abstract T parseAndAddGap(T value, String gap)
        throws java.text.ParseException;

  }

  private static class FloatRangeEndpointCalculator
      extends RangeEndpointCalculator<Float> {

    public FloatRangeEndpointCalculator(final SchemaField f) { super(f); }
    @Override
    protected Float parseVal(String rawval) {
      return Float.valueOf(rawval);
    }
    @Override
    public Float parseAndAddGap(Float value, String gap) {
      return new Float(value.floatValue() + Float.valueOf(gap).floatValue());
    }
  }
  private static class DoubleRangeEndpointCalculator
      extends RangeEndpointCalculator<Double> {

    public DoubleRangeEndpointCalculator(final SchemaField f) { super(f); }
    @Override
    protected Double parseVal(String rawval) {
      return Double.valueOf(rawval);
    }
    @Override
    public Double parseAndAddGap(Double value, String gap) {
      return new Double(value.doubleValue() + Double.valueOf(gap).doubleValue());
    }
  }
  private static class IntegerRangeEndpointCalculator
      extends RangeEndpointCalculator<Integer> {

    public IntegerRangeEndpointCalculator(final SchemaField f) { super(f); }
    @Override
    protected Integer parseVal(String rawval) {
      return Integer.valueOf(rawval);
    }
    @Override
    public Integer parseAndAddGap(Integer value, String gap) {
      return new Integer(value.intValue() + Integer.valueOf(gap).intValue());
    }
  }
  private static class LongRangeEndpointCalculator
      extends RangeEndpointCalculator<Long> {

    public LongRangeEndpointCalculator(final SchemaField f) { super(f); }
    @Override
    protected Long parseVal(String rawval) {
      return Long.valueOf(rawval);
    }
    @Override
    public Long parseAndAddGap(Long value, String gap) {
      return new Long(value.longValue() + Long.valueOf(gap).longValue());
    }
  }
  private static class DateRangeEndpointCalculator
      extends RangeEndpointCalculator<Date> {
    private final Date now;
    public DateRangeEndpointCalculator(final SchemaField f,
                                       final Date now) {
      super(f);
      this.now = now;
      if (! (field.getType() instanceof DateField) ) {
        throw new IllegalArgumentException
            ("SchemaField must use filed type extending DateField");
      }
    }
    @Override
    public String formatValue(Date val) {
      return ((DateField)field.getType()).toExternal(val);
    }
    @Override
    protected Date parseVal(String rawval) {
      return ((DateField)field.getType()).parseMath(now, rawval);
    }
    @Override
    protected Object parseGap(final String rawval) {
      return rawval;
    }
    @Override
    public Date parseAndAddGap(Date value, String gap) throws java.text.ParseException {
      final DateMathParser dmp = new DateMathParser();
      dmp.setNow(value);
      return dmp.parseMath(gap);
    }
  }

}


// idea: somehow reuse QParserPlugin framework?
// Or ValueSourceParser plugin?

abstract class FacetParser<FacetRequestT extends FacetRequest> {
  protected FacetRequestT facet;
  protected FacetParser parent;
  protected String key;

  public FacetParser(FacetParser parent,String key) {
    this.parent = parent;
    this.key = key;
  }

  public String getKey() {
    return key;
  }

  public String getPathStr() {
    if (parent == null) {
      return "/" + key;
    }
    return parent.getKey() + "/" + key;
  }

  protected RuntimeException err(String msg) {
    return new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg + " ,path="+getPathStr());
  }

  public abstract FacetRequest parse(Object o) throws SyntaxError;

  // TODO: put the FacetRequest on the parser object?
  public void parseSubs(Object o) throws SyntaxError {
    if (o==null) return;
    if (o instanceof Map) {
      Map<String,Object> m = (Map<String, Object>) o;
      for (Map.Entry<String,Object> entry : m.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();

        // "my_prices" : { "range" : { "field":...
        // key="my_prices", value={"range":..

        Object parsedValue = parseFacetOrStat(key, value);

        // TODO: have parseFacetOrStat directly add instead of return!!! nocommit
        if (parsedValue instanceof FacetRequest) {
          facet.addSubFacet(key, (FacetRequest)parsedValue);
        } else if (parsedValue instanceof AggValueSource) {
          facet.addStat(key, (AggValueSource)parsedValue);
        } else {
          throw new RuntimeException("Huh? TODO: " + parsedValue);
        }
      }
    } else {
      // facet : my_field?
      throw err("Expected map for facet/stat");
    }
  }

  public Object parseFacetOrStat(String key, Object o) throws SyntaxError {
   if (o instanceof String) {
     return parseStringFacetOrStat(key, (String)o);
   }

   if (!(o instanceof Map)) {
     throw err("expected Map but got " + o);
   }

   // { "range" : { "field":...
  Map<String,Object> m = (Map<String,Object>)o;
  if (m.size() != 1) {
    throw err("expected facet/stat type name, like {range:{... but got " + m);
  }

    // Is this most efficient way?
    Map.Entry<String,Object> entry = m.entrySet().iterator().next();
    String type = entry.getKey();
    Object args = entry.getValue();
    return parseFacetOrStat(key, type, args);
  }

  public Object parseFacetOrStat(String key, String type, Object args) throws SyntaxError {
    // TODO: a place to register all these facet types?

    if ("field".equals(type) || "terms".equals(type)) {
      return parseFieldFacet(key, args);
    } else if ("query".equals(type)) {
      return parseQueryFacet(key, args);
    } else if ("range".equals(type)) {
     return parseRangeFacet(key, args);
    }

    return parseStat(key, type, args);
  }



  FacetField parseFieldFacet(String key, Object args) throws SyntaxError {
    FacetFieldParser parser = new FacetFieldParser(this, key);
    return parser.parse(args);
  }

  FacetQuery parseQueryFacet(String key, Object args) throws SyntaxError {
    FacetQueryParser parser = new FacetQueryParser(this, key);
    return parser.parse(args);
  }

  FacetRange parseRangeFacet(String key, Object args) throws SyntaxError {
    FacetRangeParser parser = new FacetRangeParser(this, key);
    return parser.parse(args);
  }

  public Object parseStringFacetOrStat(String key, String s) throws SyntaxError {
    // "avg(myfield)"
    return parseStringStat(key, s);
    // TODO - simple string representation of facets
  }

  // parses avg(x)
  private AggValueSource parseStringStat(String key, String stat) throws SyntaxError {
    FunctionQParser parser = (FunctionQParser)QParser.getParser(stat, FunctionQParserPlugin.NAME, getSolrRequest());
    AggValueSource agg = parser.parseAgg(FunctionQParser.FLAG_DEFAULT);
    return agg;
  }

  public AggValueSource parseStat(String key, String type, Object args) throws SyntaxError {
    return null;
  }


  public String getField(Map<String,Object> args) {
    Object fieldName = args.get("field"); // TODO: pull out into defined constant
    if (fieldName == null) {
      fieldName = args.get("f");  // short form
    }
    if (fieldName == null) {
      throw err("Missing 'field'");
    }

    if (!(fieldName instanceof String)) {
      throw err("Expected string for 'field', got" + fieldName);
    }

    return (String)fieldName;
  }


  public Long getLongOrNull(Map<String,Object> args, String paramName, boolean required) {
    Object o = args.get(paramName);
    if (o == null) {
      if (required) {
        throw err("Missing required parameter '" + paramName + "'");
      }
      return null;
    }
    if (!(o instanceof Long || o instanceof Integer || o instanceof Short || o instanceof Byte)) {
      throw err("Expected integer type for param '"+paramName + "' but got " + o);
    }

    return ((Number)o).longValue();
  }

  public long getLong(Map<String,Object> args, String paramName, long defVal) {
    Object o = args.get(paramName);
    if (o == null) {
      return defVal;
    }
    if (!(o instanceof Long || o instanceof Integer || o instanceof Short || o instanceof Byte)) {
      throw err("Expected integer type for param '"+paramName + "' but got " + o.getClass().getSimpleName() + " = " + o);
    }

    return ((Number)o).longValue();
  }

  public boolean getBoolean(Map<String,Object> args, String paramName, boolean defVal) {
    Object o = args.get(paramName);
    if (o == null) {
      return defVal;
    }
    // TODO: should we be more flexible and accept things like "true" (strings)?
    // Perhaps wait until the use case comes up.
    if (!(o instanceof Boolean)) {
      throw err("Expected boolean type for param '"+paramName + "' but got " + o.getClass().getSimpleName() + " = " + o);
    }

    return (Boolean)o;
  }

  public String getString(Map<String,Object> args, String paramName, String defVal) {
    Object o = args.get(paramName);
    if (o == null) {
      return defVal;
    }
    if (!(o instanceof String)) {
      throw err("Expected string type for param '"+paramName + "' but got " + o.getClass().getSimpleName() + " = " + o);
    }

    return (String)o;
  }


  public IndexSchema getSchema() {
    return parent.getSchema();
  }

  public SolrQueryRequest getSolrRequest() {
    return parent.getSolrRequest();
  }

}


class FacetTopParser extends FacetParser<FacetQuery> {
  private SolrQueryRequest req;

  public FacetTopParser(SolrQueryRequest req) {
    super(null, "facet");
    this.facet = new FacetQuery();
    this.req = req;
  }

  @Override
  public FacetQuery parse(Object args) throws SyntaxError {
    parseSubs(args);
    return facet;
  }

  @Override
  public SolrQueryRequest getSolrRequest() {
    return req;
  }

  @Override
  public IndexSchema getSchema() {
    return req.getSchema();
  }
}

class FacetQueryParser extends FacetParser<FacetQuery> {
  public FacetQueryParser(FacetParser parent, String key) {
    super(parent, key);
    facet = new FacetQuery();
  }

  @Override
  public FacetQuery parse(Object arg) throws SyntaxError {
    String qstring = null;
    if (arg instanceof String) {
      // just the field name...
      qstring = (String)arg;

    } else if (arg instanceof Map) {
      Map<String, Object> m = (Map<String, Object>) arg;
      qstring = getString(m, "q", null);
      if (qstring == null) {
        qstring = getString(m, "query", null);
      }

      // OK to parse subs before we have parsed our own query?
      // as long as subs don't need to know about it.
      parseSubs( m.get("facet") );
    }

    // TODO: substats that are from defaults!!!

    if (qstring != null) {
      QParser parser = QParser.getParser(qstring, null, getSolrRequest());
      facet.q = parser.getQuery();
    }

    return facet;
  }
}

class FacetFieldParser extends FacetParser<FacetField> {
  public FacetFieldParser(FacetParser parent, String key) {
    super(parent, key);
    facet = new FacetField();
  }

  public FacetField parse(Object arg) throws SyntaxError {
    // set defaults
    facet.offset = 0;
    facet.limit = 10;
    facet.mincount = 1;
    facet.missing = false;


    if (arg instanceof String) {
      // just the field name...
      facet.field = (String)arg;
      parseSort( null );  // TODO: defaults

    } else if (arg instanceof Map) {
      Map<String, Object> m = (Map<String, Object>) arg;
      facet.field = getField(m);
      facet.offset = getLong(m, "offset", facet.offset);
      facet.limit = getLong(m, "limit", facet.limit);
      facet.mincount = getLong(m, "mincount", facet.mincount);
      facet.missing = getBoolean(m, "missing", facet.missing);
      facet.prefix = getString(m, "prefix", facet.prefix);
      facet.allBuckets = getBoolean(m, "allBuckets", facet.allBuckets);
      facet.method = FacetField.FacetMethod.fromString( getString(m, "method", null) );

      // facet.sort may depend on a facet stat...
      // should we be parsing / validating this here, or in the execution environment?
      Object o = m.get("facet");
      parseSubs(o);

      parseSort( m.get("sort") );
    }

    return facet;
  }


  // Sort specification is currently
  // sort : 'mystat desc'
  // OR
  // sort : { mystat : 'desc' }
  private void parseSort(Object sort) {
    if (sort == null) {
      facet.sortVariable = "count";
      facet.sortDirection = FacetField.SortDirection.desc;
    } else if (sort instanceof String) {
      String sortStr = (String)sort;
      if (sortStr.endsWith(" asc")) {
        facet.sortVariable = sortStr.substring(0, sortStr.length()-" asc".length());
        facet.sortDirection = FacetField.SortDirection.asc;
      } else if (sortStr.endsWith(" desc")) {
        facet.sortVariable = sortStr.substring(0, sortStr.length()-" desc".length());
        facet.sortDirection = FacetField.SortDirection.desc;
      } else {
        facet.sortDirection = "index".equals(facet.sortVariable) ? FacetField.SortDirection.asc : FacetField.SortDirection.desc;  // default direction for "index" is ascending
      }
    } else {
     // sort : { myvar : 'desc' }
      Map<String,Object> map = (Map<String,Object>)sort;
      // TODO: validate
      Map.Entry<String,Object> entry = map.entrySet().iterator().next();
      String k = entry.getKey();
      Object v = entry.getValue();
      facet.sortVariable = k;
      facet.sortDirection = FacetField.SortDirection.valueOf(v.toString());
    }

  }
}



class FacetRangeParser extends FacetParser<FacetRange> {
  public FacetRangeParser(FacetParser parent, String key) {
    super(parent, key);
    facet = new FacetRange();
  }

  public FacetRange parse(Object arg) throws SyntaxError {
    if (!(arg instanceof Map)) {
      throw err("Missing range facet arguments");
    }

    Map<String, Object> m = (Map<String, Object>) arg;

    facet.field = getString(m, "field", null);

    facet.start = m.get("start");
    facet.end = m.get("end");
    facet.gap = m.get("gap");
    facet.hardend = getBoolean(m, "hardend", facet.hardend);

    // TODO: refactor list-of-options code

    Object o = m.get("include");
    String[] includeList = null;
    if (o != null) {
      List lst = null;

      if (o instanceof List) {
        lst = (List)o;
      } else if (o instanceof String) {
        lst = StrUtils.splitSmart((String)o, ',');
      }

      includeList = (String[])lst.toArray(new String[lst.size()]);
    }
    facet.include = FacetParams.FacetRangeInclude.parseParam( includeList );

    facet.others = EnumSet.noneOf(FacetParams.FacetRangeOther.class);

    o = m.get("others");
    if (o != null) {
      List<String> lst = null;

      if (o instanceof List) {
        lst = (List)o;
      } else if (o instanceof String) {
        lst = StrUtils.splitSmart((String)o, ',');
      }

      for (String otherStr : lst) {
        facet.others.add( FacetParams.FacetRangeOther.get(otherStr) );
      }
    }


    Object facetObj = m.get("facet");
    parseSubs(facetObj);

    return facet;
  }

}


class FacetQuery extends FacetRequest {
  // query string or query?
  Query q;

  @Override
  public FacetProcessor createFacetProcessor(FacetContext fcontext) {
    return new FacetQueryProcessor(fcontext, this);
  }
}


class FacetRange extends FacetRequest {
  String field;
  Object start;
  Object end;
  Object gap;
  boolean hardend = false;
  EnumSet<FacetParams.FacetRangeInclude> include;
  EnumSet<FacetParams.FacetRangeOther> others;

  @Override
  public FacetProcessor createFacetProcessor(FacetContext fcontext) {
    return new FacetRangeProcessor(fcontext, this);
  }
}




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
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.field.FieldUtil;
import org.apache.solr.search.function.FuncValues;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.mutable.MutableValueInt;

import java.io.Closeable;
import java.io.IOException;


public abstract class SlotAcc implements Closeable {
  String key; // todo...
  protected final FacetContext fcontext;

  public SlotAcc(FacetContext fcontext) {
    this.fcontext = fcontext;
  }

  public void setNextReader(AtomicReaderContext readerContext) throws IOException {
  }

  public abstract void collect(int doc, int slot) throws IOException;

  public abstract int compare(int slotA, int slotB);

  public abstract Comparable getValue(int slotNum);

  public void setValues(NamedList<Object> bucket, int slotNum) {
    if (key == null) return;
    bucket.add(key, getValue(slotNum));
  }

  public abstract void reset();

  @Override
  public void close() throws IOException {
  }
}


// TODO: we should really have a decoupled value provider...
// This would enhance reuse and also prevent multiple lookups of same value across diff stats
abstract class FuncSlotAcc extends SlotAcc {
  protected final ValueSource valueSource;
  protected FuncValues values;

  public FuncSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
    super(fcontext);
    this.valueSource = values;
  }

  @Override
  public void setNextReader(AtomicReaderContext readerContext) throws IOException {
    values = valueSource.getValues(fcontext.qcontext, readerContext);
  }
}


// have a version that counts the number of times a Slot has been hit?  (for avg... what else?)

// TODO: make more sense to have func as the base class rather than double?
// double-slot-func -> func-slot -> slot -> acc
// double-slot-func -> double-slot -> slot -> acc

abstract class DoubleFuncSlotAcc extends FuncSlotAcc {
  double[] result;  // TODO: use DoubleArray
  double initialValue;

  public DoubleFuncSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
    this(values, fcontext, numSlots, 0);
  }

  public DoubleFuncSlotAcc(ValueSource values, FacetContext fcontext, int numSlots, double initialValue) {
    super(values, fcontext, numSlots);
    this.initialValue = initialValue;
    result = new double[numSlots];
    if (initialValue != 0) {
      reset();
    }
  }

  @Override
  public int compare(int slotA, int slotB) {
    return Double.compare(result[slotA], result[slotB]);
  }


  @Override
  public Double getValue(int slot) {
    return result[slot];
  }

  @Override
  public void reset() {
    for (int i=0; i<result.length; i++) {
      result[i] = initialValue;
    }
  }
}

abstract class IntSlotAcc extends SlotAcc {
  int[] result;  // use LongArray32
  int initialValue;

  public IntSlotAcc(FacetContext fcontext, int numSlots, int initialValue) {
    super(fcontext);
    this.initialValue = initialValue;
    result = new int[numSlots];
    if (initialValue != 0) {
      reset();
    }
  }

  @Override
  public int compare(int slotA, int slotB) {
    return Integer.compare(result[slotA], result[slotB]);
  }

  @Override
  public Integer getValue(int slot) {
    return result[slot];
  }

  @Override
  public void reset() {
    for (int i=0; i<result.length; i++) {
      result[i] = initialValue;
    }
  }
}



class SumSlotAcc extends DoubleFuncSlotAcc {
  public SumSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
    super(values, fcontext, numSlots);
  }

  public void collect(int doc, int slotNum) {
    double val = values.doubleVal(doc);  // todo: worth trying to share this value across multiple stats that need it?
    result[slotNum] += val;
  }
}

class SumsqSlotAcc extends DoubleFuncSlotAcc {
  public SumsqSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
    super(values, fcontext, numSlots);
  }

  @Override
  public void collect(int doc, int slotNum) {
    double val = values.doubleVal(doc);
    val = val * val;
    result[slotNum] += val;
  }
}



class MinSlotAcc extends DoubleFuncSlotAcc {
  public MinSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
    super(values, fcontext, numSlots, Double.NaN);
  }

  @Override
  public void collect(int doc, int slotNum) {
    double val = values.doubleVal(doc);
    if (val == 0 && !values.exists(doc)) return;  // depend on fact that non existing values return 0 for func query

    double currMin = result[slotNum];
    if (!(val >= currMin)) {  // val>=currMin will be false for staring value: val>=NaN
      result[slotNum] = val;
    }
  }
}

class MaxSlotAcc extends DoubleFuncSlotAcc {
  public MaxSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
    super(values, fcontext, numSlots, Double.NaN);
  }

  @Override
  public void collect(int doc, int slotNum) {
    double val = values.doubleVal(doc);
    if (val == 0 && !values.exists(doc)) return;  // depend on fact that non existing values return 0 for func query

    double currMax = result[slotNum];
    if (!(val <= currMax)) {  // reversed order to handle NaN
      result[slotNum] = val;
    }
  }

}


class AvgSlotAcc extends DoubleFuncSlotAcc {
  int[] counts;

  public AvgSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
    super(values, fcontext, numSlots);
    counts = new int[numSlots];
  }

  @Override
  public void reset() {
    super.reset();
    for (int i=0; i<counts.length; i++) {
      counts[i] = 0;
    }
  }

  @Override
  public void collect(int doc, int slotNum) {
    double val = values.doubleVal(doc);  // todo: worth trying to share this value across multiple stats that need it?
    result[slotNum] += val;
    counts[slotNum] += 1;
  }

  private double avg(double tot, int count) {
    return count==0 ? 0 : tot/count;  // returns 0 instead of NaN.. todo - make configurable? if NaN, we need to handle comparisons though...
  }

  private double avg(int slot) {
    return avg(result[slot], counts[slot]);  // calc once and cache in result?
  }

  @Override
  public int compare(int slotA, int slotB) {
    return Double.compare(avg(slotA), avg(slotB));
  }

  @Override
  public Double getValue(int slot) {
    return avg(slot);
  }

}



class CountSlotAcc extends IntSlotAcc {
  public CountSlotAcc(FacetContext fcontext, int numSlots) {
    super(fcontext, numSlots, 0);
  }

  @Override
  public void collect(int doc, int slotNum) {       // TODO: count arrays can use fewer bytes based on the number of docs in the base set (that's the upper bound for single valued) - look at ttf?
    result[slotNum] = result[slotNum] + 1;
  }

  public void incrementCount(int slot, int count) {
    result[slot] += count;
  }

  public int getCount(int slot) {
    return result[slot];
  }

  @Override
  public void reset() {
    super.reset();
  }
}


class SortSlotAcc extends SlotAcc {
  public SortSlotAcc(FacetContext fcontext) {
    super(fcontext);
  }

  @Override
  public void collect(int doc, int slot) throws IOException {
    // no-op
  }

  public int compare(int slotA, int slotB) {
    return slotA - slotB;
  }

  @Override
  public Comparable getValue(int slotNum) {
    return slotNum;
  }

  @Override
  public void reset() {
    // no-op
  }
}


abstract class UniqueSlotAcc extends SlotAcc {
  FixedBitSet[] arr;
  int currentDocBase;
  int[] counts;  // populated with the cardinality once
  int nTerms;

  public UniqueSlotAcc(FacetContext fcontext, String field, int numSlots) throws IOException {
    super(fcontext);
    arr = new FixedBitSet[numSlots];
  }

  @Override
  public void reset() {
    counts = null;
    for (FixedBitSet bits : arr) {
      if (bits == null) continue;
      bits.clear(0, bits.length());
    }
  }

  @Override
  public void setNextReader(AtomicReaderContext readerContext) throws IOException {
    currentDocBase = readerContext.docBase;
  }

  @Override
  public Comparable getValue(int slot) {
    if (counts != null) {  // will only be pre-populated if this was used for sorting.
      return counts[slot];
    }

    FixedBitSet bs = arr[slot];
    return bs==null ? 0 : bs.cardinality();
  }


  // we only calculate all the counts when sorting by count
  public void calcCounts() {
    counts = new int[arr.length];
    for (int i=0; i<arr.length; i++) {
      FixedBitSet bs = arr[i];
      counts[i] = bs == null ? 0 : bs.cardinality();
    }
  }

  @Override
  public int compare(int slotA, int slotB) {
    if (counts == null) {  // TODO: a more efficient way to do this?  prepareSort?
      calcCounts();
    }
    return counts[slotA] - counts[slotB];
  }

}


class UniqueSinglevaluedSlotAcc extends UniqueSlotAcc {
  SortedDocValues si;

  public UniqueSinglevaluedSlotAcc(FacetContext fcontext, String field, int numSlots) throws IOException {
    super(fcontext, field, numSlots);
    SolrIndexSearcher searcher = fcontext.qcontext.searcher();
    si = FieldUtil.getSortedDocValues(fcontext.qcontext, searcher.getSchema().getField(field), null);
    nTerms = si.getValueCount();
  }

  @Override
  public void collect(int doc, int slotNum) {
    int ord = si.getOrd(doc + currentDocBase);
    if (ord < 0) return;  // -1 means missing

    FixedBitSet bits = arr[slotNum];
    if (bits == null) {
      bits = new FixedBitSet(nTerms);
      arr[slotNum] = bits;
    }
    bits.set(ord);
  }
}


class UniqueMultivaluedSlotAcc extends UniqueSlotAcc implements UnInvertedField.Callback {
  private UnInvertedField uif;
  private UnInvertedField.DocToTerm docToTerm;

  public UniqueMultivaluedSlotAcc(FacetContext fcontext, String field, int numSlots) throws IOException {
    super(fcontext, field, numSlots);
    SolrIndexSearcher searcher = fcontext.qcontext.searcher();
    uif = UnInvertedField.getUnInvertedField(field, searcher);
    docToTerm = uif.new DocToTerm();
    nTerms = uif.numTerms();
  }


  private FixedBitSet bits;  // bits for the current slot, only set for the callback
  @Override
  public void call(int termNum) {
    bits.set(termNum);
  }

  @Override
  public void collect(int doc, int slotNum) throws IOException {
    FixedBitSet bs = null;
    bs = arr[slotNum];
    if (bs == null) {
      bs = new FixedBitSet(nTerms);
      arr[slotNum] = bs;
    }
    this.bits = bs;
    docToTerm.getTerms(doc + currentDocBase, this);  // this will call back to our Callback.call(int termNum)
  }

  @Override
  public void close() throws IOException {
    if (docToTerm != null) {
      docToTerm.close();
      docToTerm = null;
    }
  }
}
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
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.field.FieldUtil;
import org.apache.solr.search.function.FuncValues;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.mutable.MutableValueInt;

import java.io.IOException;

public abstract class SlotAcc extends Acc {
  protected final MutableValueInt slot;

  public SlotAcc(MutableValueInt slot) {
    this.slot = slot;
  }

  public abstract int compare(int slotA, int slotB);

  public Comparable getValue(int slotNum) {
    slot.value = slotNum;
    return getValue();
  }

  public abstract Comparable getGlobalValue();

  public void setValues(NamedList<Object> bucket, int slotNum) {
    if (key == null) return;
    if (slotNum == -1) {
      bucket.add(key, getGlobalValue());
    } else {
      bucket.add(key, getValue(slotNum));
    }
  }

  public abstract void reset();
}



// TODO: we should really have a decoupled value provider...
// This would enhance reuse and also prevent multiple lookups of same value across diff stats
abstract class FuncSlotAcc extends SlotAcc {
  ValueSource valueSource;
  QueryContext queryContext;
  FuncValues values;

  public FuncSlotAcc(MutableValueInt slot, ValueSource values, QueryContext queryContext, int numSlots) {
    super(slot);
    this.valueSource = values;
    this.queryContext = queryContext;
  }

  @Override
  public void setNextReader(AtomicReaderContext readerContext) throws IOException {
    values = valueSource.getValues(queryContext, readerContext);
    super.setNextReader(readerContext);
  }
}


// have a version that counts the number of times a Slot has been hit?  (for avg... what else?)

// TODO: make more sense to have func as the base class rather than double?
// double-slot-func -> func-slot -> slot -> acc
// double-slot-func -> double-slot -> slot -> acc

abstract class DoubleFuncSlotAcc extends FuncSlotAcc {
  double[] result;  // TODO: use DoubleArray
  double initialValue;

  public DoubleFuncSlotAcc(MutableValueInt slot, ValueSource values, QueryContext queryContext, int numSlots) {
    this(slot, values, queryContext, numSlots, 0);
  }
  public DoubleFuncSlotAcc(MutableValueInt slot, ValueSource values, QueryContext queryContext, int numSlots, double initialValue) {
    super(slot, values, queryContext, numSlots);
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
  public Double getValue() {
    return result[slot.value];
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

  public IntSlotAcc(MutableValueInt slot, int numSlots, int initialValue) {
    super(slot);
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
  public Integer getValue() {
    return result[slot.value];
  }

  @Override
  public void reset() {
    for (int i=0; i<result.length; i++) {
      result[i] = initialValue;
    }
  }
}





class SumSlotAcc extends DoubleFuncSlotAcc {
  double total = 0;

  public SumSlotAcc(MutableValueInt slot, ValueSource values, QueryContext queryContext, int numSlots) {
    super(slot, values, queryContext, numSlots);
  }

  public void collect(int doc) {
    double val = values.doubleVal(doc);  // todo: worth trying to share this value across multiple stats that need it?
    int slotNum = slot.value;  // todo: more efficient to pass it in?
    total += val;
    result[slotNum] += val;
  }

  public Comparable getGlobalValue() {
    return total;
  }
}

class SumsqSlotAcc extends DoubleFuncSlotAcc {
  double total = 0;

  public SumsqSlotAcc(MutableValueInt slot, ValueSource values, QueryContext queryContext, int numSlots) {
    super(slot, values, queryContext, numSlots);
  }

  public void collect(int doc) {
    double val = values.doubleVal(doc);
    int slotNum = slot.value;

    val = val * val;
    total += val;
    result[slotNum] += val;
  }

  public Comparable getGlobalValue() {
    return total;
  }
}



class MinSlotAcc extends DoubleFuncSlotAcc {
  double min = 0;

  public MinSlotAcc(MutableValueInt slot, ValueSource values, QueryContext queryContext, int numSlots) {
    super(slot, values, queryContext, numSlots, Double.NaN);
  }

  public void collect(int doc) {
    double val = values.doubleVal(doc);
    if (val == 0 && !values.exists(doc)) return;  // depend on fact that non existing values return 0 for func query

    int slotNum = slot.value;
    double currMin = result[slotNum];
    if (!(val >= currMin)) {  // val>=currMin will be false for staring value: val>=NaN
      result[slotNum] = val;
    }
    if (!(val >= min)) {
      min = val;
    }
  }

  public Comparable getGlobalValue() {
    return min;
  }
}

class MaxSlotAcc extends DoubleFuncSlotAcc {
  double max = 0;

  public MaxSlotAcc(MutableValueInt slot, ValueSource values, QueryContext queryContext, int numSlots) {
    super(slot, values, queryContext, numSlots, Double.NaN);
  }

  public void collect(int doc) {
    double val = values.doubleVal(doc);
    if (val == 0 && !values.exists(doc)) return;  // depend on fact that non existing values return 0 for func query

    int slotNum = slot.value;
    double currMax = result[slotNum];
    if (!(val <= currMax)) {  // reversed order to handle NaN
      result[slotNum] = val;
    }
    if (!(val <= max)) {
      max = val;
    }
  }

  public Comparable getGlobalValue() {
    return max;
  }
}


class AvgSlotAcc extends DoubleFuncSlotAcc {
  double tot = 0;
  int count = 0;
  int[] counts;

  public AvgSlotAcc(MutableValueInt slot, ValueSource values, QueryContext queryContext, int numSlots) {
    super(slot, values, queryContext, numSlots);
    counts = new int[numSlots];
  }

  @Override
  public void reset() {
    super.reset();
    tot = 0;
    count = 0;
    for (int i=0; i<counts.length; i++) {
      counts[i] = 0;
    }
  }

  public void collect(int doc) {
    double val = values.doubleVal(doc);  // todo: worth trying to share this value across multiple stats that need it?
    int slotNum = slot.value;  // todo: more efficient to pass it in?
    tot += val;
    result[slotNum] += val;
    count += 1;
    counts[slotNum] += 1;
  }

  private double avg(double tot, int count) {
    return count==0 ? 0 : tot/count;  // returns 0 instead of NaN.. todo - make configurable? if NaN, we need to handle comparisons though...
  }

  public Comparable getGlobalValue() {
    return avg(tot, count);
  }

  private double avg(int slot) {
    return avg(result[slot], counts[slot]);  // calc once and cache in result?
  }

  @Override
  public int compare(int slotA, int slotB) {
    return Double.compare(avg(slotA), avg(slotB));
  }


  @Override
  public Double getValue() {
    return avg(slot.value);
  }

}



class CountSlotAcc extends IntSlotAcc {
  int total = 0;

  public CountSlotAcc(MutableValueInt slot, QueryContext qContext, int numSlots) {
    super(slot, numSlots, 0);
  }

  public void collect(int doc) {       // TODO: count arrays can use fewer bytes based on the number of docs in the base set (that's the upper bound for single valued) - look at ttf?
    total++;
    int slotNum = slot.value;
    result[slotNum] = result[slotNum] + 1;
  }

  public Comparable getGlobalValue() {
    return total;
  }

  public void incrementCount(int slot, int count) {
    total += count;
    result[slot] += count;
  }

  public int getCount(int slot) {
    if (slot == -1) return total;
    return result[slot];
  }

  @Override
  public void reset() {
    total = 0;
    super.reset();
  }
}


class SortSlotAcc extends SlotAcc {
  public SortSlotAcc(MutableValueInt slot) {
    super(slot);
  }

  public int compare(int slotA, int slotB) {
    return slotA - slotB;
  }

  public Comparable getValue(int slotNum) {
    slot.value = slotNum;
    return getValue();
  }

  @Override
  public Comparable getGlobalValue() {
    return 0;
  }

  public void setValues(NamedList<Object> bucket, int slotNum) {
    if (key == null) return;
    bucket.add(key, getValue(slotNum));
  }

  @Override
  public void reset() {
    // no-op
  }

  @Override
  public Comparable getValue() {
    return slot.value;
  }
}


abstract class UniqueSlotAcc extends SlotAcc {
  FixedBitSet ords;
  FixedBitSet[] arr;
  int currentDocBase;
  int[] counts;
  int nTerms;


  public UniqueSlotAcc(MutableValueInt slot, QueryContext qContext, String field, int numSlots) throws IOException {
    super(slot);
    arr = new FixedBitSet[numSlots];
  }

  @Override
  public void reset() {
    counts = null;
    for (FixedBitSet bits : arr) {
      if (bits == null) continue;
      bits.clear(0, bits.length());
    }
    ords.clear( ords.cardinality() );
  }

  @Override
  public void setNextReader(AtomicReaderContext readerContext) throws IOException {
    currentDocBase = readerContext.docBase;
  }

  @Override
  public Comparable getValue() {
    if (counts != null) {  // will only be pre-populated if this was used for sorting.
      return counts[slot.value];
    }

    FixedBitSet bs = arr[slot.value];
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

  public Comparable getGlobalValue() {
    return ords.cardinality();
  }
}


class UniqueSinglevaluedSlotAcc extends UniqueSlotAcc {
  SortedDocValues si;

  public UniqueSinglevaluedSlotAcc(MutableValueInt slot, QueryContext qContext, String field, int numSlots) throws IOException {
    super(slot, qContext, field, numSlots);
    SolrIndexSearcher searcher = qContext.searcher();
    si = FieldUtil.getSortedDocValues(qContext, searcher.getSchema().getField(field), null);
    nTerms = si.getValueCount();
    ords = new FixedBitSet(nTerms);
  }

  public void collect(int doc) {
    int ord = si.getOrd(doc + currentDocBase);
    if (ord < 0) return;  // -1 means missing
    ords.set(ord);

    int slotNum = slot.value;
    if (slotNum < 0) return;

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

  public UniqueMultivaluedSlotAcc(MutableValueInt slot, QueryContext qContext, String field, int numSlots) throws IOException {
    super(slot, qContext, field, numSlots);
    SolrIndexSearcher searcher = qContext.searcher();
    uif = UnInvertedField.getUnInvertedField(field, searcher);
    docToTerm = uif.new DocToTerm();
    nTerms = uif.numTerms();
    ords = new FixedBitSet(nTerms);
  }


  FixedBitSet bits;  // bits for the current slot, only set for the callback
  @Override
  public void call(int termNum) {
    ords.set(termNum);
    if (bits != null) {
      bits.set(termNum);
    }
  }

  public void collect(int doc) throws IOException {
    int slotNum = slot.value;
    FixedBitSet bs = null;
    if (slotNum >= 0) {
      bs = arr[slotNum];
      if (bs == null) {
        bs = new FixedBitSet(nTerms);
        arr[slotNum] = bs;
      }
    }

    this.bits = bs;
    docToTerm.getTerms(doc + currentDocBase, this);
  }

  @Override
  public void close() throws IOException {
    if (docToTerm != null) {
      docToTerm.close();
      docToTerm = null;
    }
  }
}
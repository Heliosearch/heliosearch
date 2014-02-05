package org.apache.solr.search.field;

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
import org.apache.solr.core.RefCountBase;
import org.apache.solr.search.CacheRegenerator;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrIndexSearcher;
import org.noggit.JSONUtil;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;


public abstract class TopValues extends RefCountBase {
  protected FieldValues fieldValues;  // pointer back to the "source"

  protected LeafValues[] leafValues;
  protected volatile int nSegs;  // number of segments instantiated
  protected int carriedOver;    // number of segments carried over

  // top terms?

  public TopValues(FieldValues fieldValues) {
    this.fieldValues = fieldValues;
  }

  public boolean allSegmentsLoaded() {
    return leafValues != null && leafValues.length == nSegs;
  }


  public LeafValues getLeafValues(QueryContext context, AtomicReaderContext readerContext) throws IOException {
    assert context != null;

    int readerOrd = readerContext.ord;

    if (allSegmentsLoaded()) {
      return leafValues[readerOrd];
    }

    LeafValues leaf;
    synchronized (this) {
      if (leafValues == null) {
        leafValues = new LeafValues[readerContext.parent.leaves().size()];
      }

      leaf = leafValues[readerOrd];
      if (leaf == null) {
        leaf = new CreationLeafValue(fieldValues);
        leafValues[readerOrd] = leaf;
        // add any flags or creation commands to the creation value
        // since another thread may actually do the creation.
      }
    }

    if (leaf instanceof CreationLeafValue) {
      synchronized (leaf) {
        CreationLeafValue create = (CreationLeafValue)leaf;
        if (create.value == null) {
          create.value = createValue(this, create, readerContext);
          synchronized (this) {
            leafValues[readerOrd] = create.value;
            nSegs++;
          }
        }
        leaf = create.value;
      }
    }
    return leaf;
  }



  public abstract LeafValues createValue(TopValues topValues, CreationLeafValue create, AtomicReaderContext readerContext) throws IOException;


  @Override
  public String toString() {
    Map<String,Object> map = new LinkedHashMap<String,Object>();
    addInfo(map);
    return JSONUtil.toJSON(map, 2);
  }

  public void addInfo(Map<String,Object> map) {
    Map<String,Object> sfMap = new LinkedHashMap<String,Object>();
    fieldValues.field.addInfo(sfMap);
    map.put("field", sfMap);
    map.put("class", this.getClass().getSimpleName());
    map.put("refcount", getRefCount());
    map.put("numSegments", nSegs);
    map.put("carriedOver", carriedOver);
    map.put("size", getSizeInBytes());
  }

  public long getSizeInBytes() {
    LeafValues[] snapshot;
    synchronized (this) {
      if (leafValues == null) return 0;
      snapshot = leafValues.clone();
    }

    long answer = 0;
    for (LeafValues leaf : snapshot) {
      if (leaf != null && !(leaf instanceof CreationLeafValue)) {
        answer += leaf.getSizeInBytes();
      }
    }

    return answer;
  }


  @Override
  public void free() {
    // Free should never be called while this TopValues is still in active use, hence
    // we do not need to worry about concurrent access any longer.
    // A final read memory barrier isn't a bad idea though...

    synchronized (this) {
      if (leafValues == null) return;

      for (LeafValues leaf : leafValues) {
        if (leaf == null) continue;
        if (leaf instanceof CreationLeafValue) {
          // This should *never* happen... it means we are freeing concurrently with someone
          // trying to instantiate a leaf.  Ref counting must be off.
          throw new RuntimeException("ERROR: Encountered " + leaf + " during free of " + this);
        }
        leaf.decref();
      }
    }
  }


  static final class CreationLeafValue extends LeafValues {
    LeafValues value;

    CreationLeafValue(FieldValues fieldValues) {
      super(fieldValues);
    }

    @Override
    public long getSizeInBytes() {
      return 0;
    }

    @Override
    public FieldStats getFieldStats() {
      return null;
    }

    @Override
    public int getRefCount() {
      return 1;
    }

    @Override
    public int incref() {
      return 1;
    }

    @Override
    public int decref() {
      return 1;
    }

    @Override
    public boolean tryIncref() {
      return true;
    }

    @Override
    public boolean tryDecref() {
      return true;
    }

    @Override
    protected void free() {
    }

  }



  public static class Regenerator implements CacheRegenerator {
    @Override
    public boolean regenerateItem(SolrIndexSearcher.WarmContext warmContext, Object oldKey, Object oldVal) throws IOException {
      TopValues newValues = ((TopValues)oldVal).create(warmContext);
      warmContext.searcher.getnCache().put((String)oldKey, newValues);
      return true;
    }
  }

  // called on the old TopValues
  public abstract IntTopValues create(SolrIndexSearcher.WarmContext warmContext);

  // called on a newly created TopValues
  public void create(SolrIndexSearcher.WarmContext warmContext, TopValues oldTopValues) {
    LeafValues[] oldLeafValues = oldTopValues.leafValues;

    if (warmContext.segmentsShared == 0 || oldLeafValues == null) {
      return;
    }

    assert oldLeafValues.length == warmContext.oldToNewOrd.length;

    leafValues = new LeafValues[warmContext.searcher.getTopReaderContext().leaves().size()];

    synchronized (oldTopValues) {  // is this really necessary here?
      for (int i=0; i<warmContext.oldToNewOrd.length; i++) {
        int newOrd = warmContext.oldToNewOrd[i];
        if (newOrd >= 0) {
          LeafValues lf = oldLeafValues[i];
          if (lf != null && !(lf instanceof CreationLeafValue)) {
            lf.incref();
            leafValues[newOrd] = lf;
            carriedOver++;
            nSegs++;
          }
        }
      }
    }
  }

}
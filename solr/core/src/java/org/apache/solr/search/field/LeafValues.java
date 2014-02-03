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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.core.RefCount;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.function.FuncValues;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class LeafValues extends FuncValues implements RefCount, Closeable {
  private final AtomicInteger refcount = new AtomicInteger(1);

  protected final FieldValues fieldValues;

  public LeafValues(FieldValues fieldValues) {
    this.fieldValues = fieldValues;
  }

  public abstract long getSizeInBytes();


  @Override
  public String toString(int doc) {
    return fieldValues.description() + '=' + strVal(doc);
  }



  public static abstract class Uninvert implements Closeable {
    int maxDoc;
    public Bits docsWithField;
    public AtomicReaderContext readerContext;
    public SchemaField field;

    public Uninvert(AtomicReaderContext readerContext, SchemaField field) {
      this.readerContext = readerContext;
      this.field = field;
      this.maxDoc = readerContext.reader().maxDoc();
    }

    public void uninvert() throws IOException {
      AtomicReader reader = readerContext.reader();

      boolean setDocsWithField = true;
      final int maxDoc = reader.maxDoc();
      Terms terms = reader.terms(field.getName());
      if (terms != null) {
        if (setDocsWithField) {
          final int termsDocCount = terms.getDocCount();
          assert termsDocCount <= maxDoc;
          if (termsDocCount == maxDoc) {
            // Fast case: all docs have this field:
            docsWithField = new Bits.MatchAllBits(maxDoc);
            setDocsWithField = false;
          }
        }

        final TermsEnum termsEnum = termsEnum(terms);

        DocsEnum docs = null;
        FixedBitSet docsWithField = null;
        while(true) {
          final BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          visitTerm(term);
          docs = termsEnum.docs(null, docs, DocsEnum.FLAG_NONE);
          while (true) {
            final int docID = docs.nextDoc();
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            }
            visitDoc(docID);
            if (setDocsWithField) {
              if (docsWithField == null) {
                // Lazy init
                this.docsWithField = docsWithField = new FixedBitSet(maxDoc);
              }
              docsWithField.set(docID);
            }
          }
        }
      }
    }

    protected abstract TermsEnum termsEnum(Terms terms) throws IOException;
    protected abstract void visitTerm(BytesRef term);
    protected abstract void visitDoc(int docID);
  }




  ////////////////////////////// RefCount methods ///////////////////

  @Override
  public int getRefCount() {
    return refcount.get();
  }

  @Override
  public int incref() {
    // debug_incref();

    int count;
    while ((count = refcount.get()) > 0) {
      if (refcount.compareAndSet(count, count+1)) {
        return count+1;
      }
    }
    throw new RuntimeException("Trying to incref freed native object " + this);
  }

  @Override
  public int decref() {
    // debug_decref();

    int count;
    while ((count = refcount.get()) > 0) {
      int newCount = count - 1;
      if (refcount.compareAndSet(count, newCount)) {
        if (newCount == 0) {
          free();
        }
        return newCount;
      }
    }

    throw new RuntimeException("Too many decrefs detected for native object " + this);
  }


  @Override
  public boolean tryIncref() {
    // debug_incref();

    int count;
    while ((count = refcount.get()) > 0) {
      if (refcount.compareAndSet(count, count+1)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean tryDecref() {
    // debug_decref();

    int count;
    while ((count = refcount.get()) > 0) {
      int newCount = count - 1;
      if (refcount.compareAndSet(count, newCount)) {
        if (newCount == 0) {
          free();
        }
        return true;
      }
    }

    return false;
  }


  protected abstract void free();

  @Override  // for Closeable
  public void close() {
    decref();
  }

}

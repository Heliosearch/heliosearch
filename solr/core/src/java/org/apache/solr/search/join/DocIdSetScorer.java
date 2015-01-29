package org.apache.solr.search.join;

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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

public class DocIdSetScorer extends Scorer {
  final DocIdSetIterator iter;
  final float score;
  int doc = -1;

  public DocIdSetScorer(Weight w, DocIdSetIterator iter, float score) throws IOException {
    super(w);
    this.score = score;
    this.iter = iter==null ? DocIdSetIterator.empty() : iter;
  }

  @Override
  public int nextDoc() throws IOException {
    return iter.nextDoc();
  }

  @Override
  public int docID() {
    return iter.docID();
  }

  @Override
  public float score() throws IOException {
    return score;
  }

  @Override
  public int freq() throws IOException {
    return 1;
  }

  @Override
  public int advance(int target) throws IOException {
    return iter.advance(target);
  }

  @Override
  public long cost() {
    return iter.cost();
  }
}

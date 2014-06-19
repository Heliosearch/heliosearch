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

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;

// TODO: perhaps make an interface instead?
public class SolrSortedDocValues extends SortedDocValues {
  @Override
  public int getOrd(int docID) {
    return 0;
  }

  @Override
  public BytesRef lookupOrd(int ord) {
    return null;
  }

  @Override
  public int getValueCount() {
    return 0;
  }

  public void lookupOrd(int ord, BytesRef target) {
    target.offset = 0;
    target.copyBytes( lookupOrd(ord) );
  }
}

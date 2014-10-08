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

package org.apache.solr.client.solrj.streaming;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Tuple {

  public final boolean EOF;
  public Map fields = new HashMap();
  public List<Tuple> children = new ArrayList();

  public Tuple(boolean EOF) {
    this.EOF = EOF;
  }

  public Tuple(Map fields, boolean EOF) {
    this(EOF);
    this.fields.putAll(fields);
  }

  public List<Tuple> getChildren() {
    return this.children;
  }

  public Object get(Object key) {
    return this.fields.get(key);
  }

  public void set(Object key, Object value) {
    this.fields.put(key, value);
  }

  public Iterator<Map.Entry> getFields() {
    return fields.entrySet().iterator();
  }
}
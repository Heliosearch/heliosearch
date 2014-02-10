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

public abstract class FieldStats {
  int numDocsWithField;
  long numUniqueValues;

  public int getNumDocsWithField() {
    return numDocsWithField;
  }
  public long getNumUniqueValues() {
    return numUniqueValues;
  }
  public abstract Object getFirstValue();
  public abstract Object getLastValue();
}

class IntFieldStats extends FieldStats {
  int firstValue;
  int lastValue;

  @Override
  public Object getFirstValue() {
    return firstValue;
  }

  @Override
  public Object getLastValue() {
    return lastValue;
  }

  public int getFirst() {
    return firstValue;
  }

  public int getLast() {
    return lastValue;
  }
}

class LongFieldStats extends FieldStats {
  long firstValue;
  long lastValue;

  @Override
  public Object getFirstValue() {
    return firstValue;
  }

  @Override
  public Object getLastValue() {
    return lastValue;
  }

  public long getFirst() {
    return firstValue;
  }

  public long getLast() {
    return lastValue;
  }
}

class FloatFieldStats extends FieldStats {
  float firstValue;
  float lastValue;

  @Override
  public Object getFirstValue() {
    return firstValue;
  }

  @Override
  public Object getLastValue() {
    return lastValue;
  }

  public float getFirst() {
    return firstValue;
  }

  public float getLast() {
    return lastValue;
  }
}

class DoubleFieldStats extends FieldStats {
  double firstValue;
  double lastValue;

  @Override
  public Object getFirstValue() {
    return firstValue;
  }

  @Override
  public Object getLastValue() {
    return lastValue;
  }

  public double getFirst() {
    return firstValue;
  }

  public double getLast() {
    return lastValue;
  }
}

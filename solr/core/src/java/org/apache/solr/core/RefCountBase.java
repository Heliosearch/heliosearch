package org.apache.solr.core;

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

import org.apache.solr.core.RefCount;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class RefCountBase implements RefCount, Closeable {
  private final AtomicInteger refcount = new AtomicInteger(1);

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

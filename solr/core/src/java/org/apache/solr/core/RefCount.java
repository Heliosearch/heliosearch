package org.apache.solr.core;

public interface RefCount {
  public int getRefCount();
  public int incref();
  public int decref();
  public boolean tryIncref();
  public boolean tryDecref();
}

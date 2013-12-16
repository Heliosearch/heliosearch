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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;

public class Diagnostics {
  protected static Logger log = LoggerFactory.getLogger(Diagnostics.class);

  public interface Callable {
    public void call(Object... data);  // data depends on the context
  }

  public static void call(Callable callable, Object... data) {
    try {
      callable.call(data);
    } catch (Throwable th) {
      log.error("TEST HOOK EXCEPTION", th);
    }
  }

  public static void logThreadDumps(String message) {
    StringBuilder sb = new StringBuilder(32768);
    if (message == null) message = "============ THREAD DUMP REQUESTED ============";
    sb.append(message);
    sb.append("\n");
    ThreadInfo[] threads = ManagementFactory.getThreadMXBean().dumpAllThreads(true, true);
    for (ThreadInfo info : threads) {
      sb.append(info);
      // sb.append("\n");
    }
    log.error(sb.toString());
  }


  public static String toString(StackTraceElement[] stackTrace, int skip) {
    StringBuilder sb = new StringBuilder(2048);
    try {
      appendStackTrace(sb, stackTrace, skip, null, null, null);
    } catch (Exception e) {
      // impossible
    }
    return sb.toString();
  }

  public static String toString(StackTraceElement[] stackTrace, String... startClass_startMethod_end) {
    String startClass = startClass_startMethod_end != null && startClass_startMethod_end.length >= 1 ? startClass_startMethod_end[0] : null;
    String startMethod = startClass_startMethod_end != null && startClass_startMethod_end.length >= 2 ? startClass_startMethod_end[1] : null;
    String endSubstring = startClass_startMethod_end != null && startClass_startMethod_end.length >= 3 ? startClass_startMethod_end[2] : null;
    StringBuilder sb = new StringBuilder(2048);
    try {
      appendStackTrace(sb, stackTrace, -1, startClass, startMethod, endSubstring);
    } catch (Exception e) {
      // impossible
    }
    return sb.toString();
  }

  public static void appendStackTrace(Appendable out, StackTraceElement[] stackTrace, int skip, String startClass, String startMethod, String endSubstring) throws IOException {
    if (startClass == null && startMethod == null && skip < 0) {
      startClass = Diagnostics.class.getName();
    }

    if (endSubstring == null) {
      endSubstring = ".solr.";
    }

    int lastInteresting = stackTrace.length;
    while (endSubstring != null && --lastInteresting >= 0) {
      StackTraceElement trace = stackTrace[lastInteresting];
      if (trace.getClassName().contains(endSubstring)) break;
    }

    int firstInteresting = skip;

    if (firstInteresting < 0 && (startClass != null || startMethod != null) ) {

      boolean matched = false;
      for (firstInteresting=0; firstInteresting < lastInteresting; firstInteresting++) {
        StackTraceElement trace = stackTrace[firstInteresting];
        String className = trace.getClassName();
        String method = trace.getMethodName();
        boolean classMatch = startClass!=null && className.contains(startClass);
        boolean methodMatch = startMethod!=null && method.contains(startMethod);

        if ( (classMatch || startClass == null) && (methodMatch || startMethod == null) )
        {
          matched = true;
          continue;
        }

        if (matched) break;
      }

      firstInteresting--;
    }

    appendStackTrace(out, stackTrace, firstInteresting, lastInteresting+1);
  }

  public static void appendStackTrace(Appendable out, StackTraceElement[] stackTrace, int start, int end) throws IOException {
    for (int i=Math.max(0,start); i<end; i++) {
      StackTraceElement trace = stackTrace[i];
      out.append("\n\t");
      appendStackTraceElement(out, trace);
    }
    out.append("\n");
  }

  public static void appendStackTraceElement(Appendable out, StackTraceElement e) throws IOException {
    out.append(e.getClassName());
    out.append('.');
    out.append(e.getMethodName());
    if (e.isNativeMethod()) {
      out.append("(Native Method)");
    } else {
      String fileName = e.getFileName();
      int lineNumber = e.getLineNumber();
      if (fileName != null) {
        out.append('(');
        out.append(e.getFileName());
        if (lineNumber > 0) {
          out.append(':');
          out.append(Integer.toString(lineNumber));
        }
        out.append(')');
      } else {
        out.append("(Unknown Source)");
      }
    }
  }


}

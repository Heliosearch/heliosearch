package org.apache.solr.util;

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


import org.apache.solr.common.util.Callback;
import org.noggit.CharArr;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class CharUtils {


  public static void splitSmart(String s, char separator, boolean decode, Callback<CharSequence> callback) {
    CharArr sb = new CharArr(32);
    int pos=0, end=s.length();
    while (pos < end) {
      char ch = s.charAt(pos++);

      if (ch == separator) {
        if (sb.length() > 0) {
          callback.callback(sb);
          sb.reset();
        }
        continue;
      }

      if (ch=='\\') {
        if (!decode) sb.write(ch);
        if (pos>=end) break;  // ERROR, or let it go?
        ch = s.charAt(pos++);
        if (decode) {
          switch(ch) {
            case 'n' : ch='\n'; break;
            case 't' : ch='\t'; break;
            case 'r' : ch='\r'; break;
            case 'b' : ch='\b'; break;
            case 'f' : ch='\f'; break;
          }
        }
      }

      sb.write(ch);
    }

    if (sb.length() > 0) {
      callback.callback(sb);
    }
  }



}

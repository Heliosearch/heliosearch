/**
 * Copyright (c) 2014, Sindice Limited. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sindicetech.siren.solr.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.junit.Test;

import com.sindicetech.siren.solr.analysis.ExtendedJsonTokenizerFactory;

import java.io.Reader;
import java.io.StringReader;
import java.util.Map;

public class TestJsonTokenizerFactory extends BaseSirenStreamTestCase {

  @Test
  public void testSimpleJsonTokenizer() throws Exception {
    final Reader reader = new StringReader("{ \"aaa\" : { \"bbb\" : \"ooo\" } }");
    final Map<String,String> args = this.getDefaultInitArgs();
    final ExtendedJsonTokenizerFactory factory = new ExtendedJsonTokenizerFactory(args);
    final Tokenizer stream = factory.create(reader);
    this.assertTokenStreamContents(stream,
        new String[] {"aaa", "bbb", "ooo"});
  }

  @Test
  public void testCharacterEncoding1() throws Exception {
    final Reader reader = new StringReader("{ \"http://test.com/M\u00F6ller\" : \"M\u00F6ller\" }");
    final Map<String,String> args = this.getDefaultInitArgs();
    final ExtendedJsonTokenizerFactory factory = new ExtendedJsonTokenizerFactory(args);
    final Tokenizer stream = factory.create(reader);
    this.assertTokenStreamContents(stream,
        new String[] {"http://test.com/Möller", "Möller"});
  }

  @Test
  public void testCharacterEncoding2() throws Exception {
    final Reader reader = new StringReader("{ \"http://test.com/Möller\" : \"Möller\" }");
    final Map<String,String> args = this.getDefaultInitArgs();
    final ExtendedJsonTokenizerFactory factory = new ExtendedJsonTokenizerFactory(args);
    final Tokenizer stream = factory.create(reader);
    this.assertTokenStreamContents(stream,
        new String[] {"http://test.com/Möller", "Möller"});
  }

}

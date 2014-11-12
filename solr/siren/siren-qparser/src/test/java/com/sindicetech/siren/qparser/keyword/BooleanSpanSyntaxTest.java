/**
 * Copyright (c) 2014, Sindice Limited. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
 * SIREn is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * SIREn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.sindicetech.siren.qparser.keyword;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.config.ConfigurationKey;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;
import com.sindicetech.siren.util.SirenTestCase;

import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler;
import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;
import com.sindicetech.siren.search.node.NodeBooleanClause;
import com.sindicetech.siren.search.node.NodeFuzzyQuery;
import com.sindicetech.siren.search.node.NodeRegexpQuery;
import com.sindicetech.siren.search.spans.BooleanSpanQuery;
import com.sindicetech.siren.search.spans.MultiTermSpanQuery;
import com.sindicetech.siren.util.XSDDatatype;

import java.util.HashMap;

import static com.sindicetech.siren.search.AbstractTestSirenScorer.BooleanClauseBuilder.*;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.BooleanQueryBuilder.bq;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.BooleanSpanQueryBuilder.bsq;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.TermSpanQueryBuilder.tsq;

public class BooleanSpanSyntaxTest extends BaseKeywordQueryParserTest {

  final HashMap<ConfigurationKey, Object> config = new HashMap<ConfigurationKey, Object>();

  public BooleanSpanSyntaxTest() {
    final HashMap<String, Analyzer> dtAnalyzers = new HashMap<String, Analyzer>();
    dtAnalyzers.put(XSDDatatype.XSD_STRING, new WhitespaceAnalyzer(LuceneTestCase.TEST_VERSION_CURRENT));
    dtAnalyzers.put("json:field", new WhitespaceAnalyzer(LuceneTestCase.TEST_VERSION_CURRENT));
    config.put(ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys.DATATYPES_ANALYZERS, dtAnalyzers);
  }

  @Test
  public void testDatatype() throws Exception {
    Query q = bsq(must(tsq("aaa")), must(tsq("bbb").setDatatype("json:field"))).getLuceneProxyQuery();
    this._assertSirenQuery(config, q, "(aaa json:field(bbb))~0");
  }

  @Test
  public void testClause() throws Exception {
    Query q = bsq(must(tsq("aaa")), must(tsq("bbb")), must(tsq("ccc"))).getLuceneProxyQuery();
    this._assertSirenQuery(config, q, "(aaa bbb ccc)~0");

    q = bsq(must(tsq("aaa")), should(tsq("bbb")), must(tsq("ccc"))).getLuceneProxyQuery();
    this._assertSirenQuery(config, q, "(+aaa bbb +ccc)~0");

    q = bsq(must(tsq("aaa")), should(tsq("bbb")), not(tsq("ccc"))).getLuceneProxyQuery();
    this._assertSirenQuery(config, q, "(+aaa bbb -ccc)~0");

    q = bsq(must(tsq("aaa")), should(tsq("bbb")), not(tsq("ccc"))).getLuceneProxyQuery();
    this._assertSirenQuery(config, q, "(+aaa bbb -ccc)~0");

    q = bsq(must(tsq("aaa")), must(tsq("bbb"))).getLuceneProxyQuery();
    this._assertSirenQuery(config, q, "(aaa AND bbb)~0");
    this._assertSirenQuery(config, q, "(aaa && bbb)~0");

    q = bsq(must(tsq("aaa")), must(tsq("bbb"))).inOrder(true).getLuceneProxyQuery();
    this._assertSirenQuery(config, q, "(aaa AND bbb)#0");
  }

  @Test
  public void testClauseDebug() throws Exception {
    Query q = bsq(must(tsq("aaa")), must(tsq("bbb"))).inOrder(true).getLuceneProxyQuery();
    this._assertSirenQuery(config, q, "(aaa AND bbb)#0");
  }

  @Test
  public void testNestedClause() throws Exception {
    Query q = bsq(must(tsq("aaa")), must(bsq(should(tsq("bbb")), should(tsq("ccc"))))).getLuceneProxyQuery();
    this._assertSirenQuery(config, q, "(+aaa +(bbb ccc)~0)~0");

    q = bsq(
      must(tsq("aaa")),
      not(bsq(
        must(tsq("bbb")),
        should(bsq(
          must(tsq("ccc")),
          should(tsq("ddd"))))
      ))).getLuceneProxyQuery();
    this._assertSirenQuery(config, q, "(+aaa -(+bbb (+ccc ddd)~0)~0)~0");
  }

  @Test(expected=org.apache.lucene.queryparser.flexible.core.QueryNodeException.class)
  public void testNestedInvalidBoolean() throws Exception {
    this._assertSirenQuery(config, null, "(+aaa +(bbb ccc))~0");
  }

  @Test
  public void testNestedBoolean() throws Exception {
    Query q = bq(must("aaa"), must(bsq(should(tsq("bbb")), should(tsq("ccc"))))).getQuery();
    this._assertSirenQuery(config, q, "(+aaa +(bbb ccc)~0)");
  }

  @Test(expected=org.apache.lucene.queryparser.flexible.standard.parser.ParseException.class)
  public void testSlop() throws Exception {
    Query q = bsq(must(tsq("aaa")), should(tsq("bbb"))).slop(1).getLuceneProxyQuery();
    this._assertSirenQuery(config, q, "(+aaa bbb)~1");

    // must throw an exception
    this._assertSirenQuery(config, q, "(+aaa bbb)~1.1");
  }

  @Test(expected=Error.class)
  public void testPhrase() throws Exception {
    // must throw an exception
    this._assertSirenQuery(config, null, "(\"aaa bbb\" AND ccc)~0");
  }

  @Test
  public void testRegexQueries() throws Exception {
    BooleanSpanQuery bsq = new BooleanSpanQuery(0, false);
    bsq.add(tsq("aaa").getQuery(), NodeBooleanClause.Occur.MUST);
    NodeRegexpQuery regexp = new NodeRegexpQuery(new Term(SirenTestCase.DEFAULT_TEST_FIELD, "s*e"));
    regexp.setDatatype("http://www.w3.org/2001/XMLSchema#string");
    bsq.add(new MultiTermSpanQuery<>(regexp), NodeBooleanClause.Occur.MUST);
    this._assertSirenQuery(new LuceneProxyNodeQuery(bsq), "(aaa AND /s*e/)~0");
  }

  @Test
  public void testFuzzyQueries() throws Exception {
    BooleanSpanQuery bsq = new BooleanSpanQuery(0, false);
    bsq.add(tsq("aaa").getQuery(), NodeBooleanClause.Occur.MUST);
    NodeFuzzyQuery fuzzy = new NodeFuzzyQuery(new Term(SirenTestCase.DEFAULT_TEST_FIELD, "bbb"));
    fuzzy.setDatatype("http://www.w3.org/2001/XMLSchema#string");
    bsq.add(new MultiTermSpanQuery<>(fuzzy), NodeBooleanClause.Occur.MUST);
    this._assertSirenQuery(new LuceneProxyNodeQuery(bsq), "(aaa AND bbb~)~0");
  }

  @Test
  public void testInOrder() throws Exception {
    Query q = bsq(must(tsq("aaa")), must(tsq("bbb")), must(tsq("ccc"))).inOrder(true).getLuceneProxyQuery();
    this._assertSirenQuery(config, q, "(aaa bbb ccc)#0");
  }

}

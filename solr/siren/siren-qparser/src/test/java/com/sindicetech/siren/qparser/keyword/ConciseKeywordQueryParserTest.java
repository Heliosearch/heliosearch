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

import com.sindicetech.siren.analysis.IntNumericAnalyzer;
import com.sindicetech.siren.qparser.keyword.builders.concise.ConciseNodeNumericRangeQuery;
import com.sindicetech.siren.search.node.*;
import com.sindicetech.siren.util.SirenTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodePhraseQueryBuilder.npq;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeTermQueryBuilder.ntq;
import static org.junit.Assert.assertEquals;

public class ConciseKeywordQueryParserTest {

  // check if empty queries are correctly converted into an attribute query
  @Test
  public void testEmptyNodeTermQuery() throws Exception {
    ConciseKeywordQueryParser parser = new ConciseKeywordQueryParser();
    parser.setAttribute("aaa");

    final NodeQuery q = ntq("aaa:").getQuery();

    assertEquals(q, parser.parse("", SirenTestCase.DEFAULT_TEST_FIELD));
  }

  // if no attribute defined, an empty query must be converted into an empty node boolean query
  @Test
  public void testEmptyNodeTermQueryWithNoAttribute() throws Exception {
    ConciseKeywordQueryParser parser = new ConciseKeywordQueryParser();

    final NodeBooleanQuery q = new NodeBooleanQuery();

    assertEquals(q, parser.parse("", SirenTestCase.DEFAULT_TEST_FIELD));
  }

  @Test
  public void testNodeTermQuery() throws Exception {
    ConciseKeywordQueryParser parser = new ConciseKeywordQueryParser();
    parser.setAttribute("aaa");

    NodeQuery q = ntq("aaa:b").getQuery();

    assertEquals(q, parser.parse("b", SirenTestCase.DEFAULT_TEST_FIELD));

    // with no attribute defined

    parser = new ConciseKeywordQueryParser();

    q = ntq("b").getQuery();

    assertEquals(q, parser.parse("b", SirenTestCase.DEFAULT_TEST_FIELD));
  }

  @Test
  public void testNodePhraseQuery() throws Exception {
    ConciseKeywordQueryParser parser = new ConciseKeywordQueryParser();
    parser.setAttribute("aaa");

    NodeQuery q = npq("aaa:b", "aaa:c").getQuery();

    assertEquals(q, parser.parse("\"b c\"", SirenTestCase.DEFAULT_TEST_FIELD));

    // with no attribute defined

    parser = new ConciseKeywordQueryParser();

    q = npq("b", "c").getQuery();

    assertEquals(q, parser.parse("\"b c\"", SirenTestCase.DEFAULT_TEST_FIELD));
  }

  @Test
  public void testRegexQueries() throws Exception {
    ConciseKeywordQueryParser parser = new ConciseKeywordQueryParser();
    parser.setAttribute("aaa");

    NodeRegexpQuery q = new NodeRegexpQuery(new Term(SirenTestCase.DEFAULT_TEST_FIELD, "aaa:b*d"));
    q.setDatatype("http://www.w3.org/2001/XMLSchema#string");

    assertEquals(q, parser.parse("/b*d/", SirenTestCase.DEFAULT_TEST_FIELD));


    // with no attribute defined

    parser = new ConciseKeywordQueryParser();

    q = new NodeRegexpQuery(new Term(SirenTestCase.DEFAULT_TEST_FIELD, "b*d"));
    q.setDatatype("http://www.w3.org/2001/XMLSchema#string");

    assertEquals(q, parser.parse("/b*d/", SirenTestCase.DEFAULT_TEST_FIELD));
  }

  @Test
  public void testTermRangeQuery() throws Exception {
    ConciseKeywordQueryParser parser = new ConciseKeywordQueryParser();
    parser.setAttribute("aaa");

    NodeQuery q = new NodeTermRangeQuery(SirenTestCase.DEFAULT_TEST_FIELD, new BytesRef("aaa:a"), new BytesRef("aaa:b"),
      true, true);

    assertEquals(q, parser.parse("[ a TO b ]", SirenTestCase.DEFAULT_TEST_FIELD));

    // with no attribute defined

    parser = new ConciseKeywordQueryParser();

    q = new NodeTermRangeQuery(SirenTestCase.DEFAULT_TEST_FIELD, new BytesRef("a"), new BytesRef("b"),
      true, true);

    assertEquals(q, parser.parse("[ a TO b ]", SirenTestCase.DEFAULT_TEST_FIELD));
  }

  @Test
  public void testFuzzyQuery() throws Exception {
    ConciseKeywordQueryParser parser = new ConciseKeywordQueryParser();
    parser.setAttribute("aaa");

    // must have a prefix length of 4
    NodeFuzzyQuery q = new NodeFuzzyQuery(new Term(SirenTestCase.DEFAULT_TEST_FIELD, "aaa:michel"), NodeFuzzyQuery.defaultMaxEdits, 4);
    q.setDatatype("http://www.w3.org/2001/XMLSchema#string");

    assertEquals(q, parser.parse("michel~", SirenTestCase.DEFAULT_TEST_FIELD));

    // with no attribute defined

    parser = new ConciseKeywordQueryParser();

    q = new NodeFuzzyQuery(new Term(SirenTestCase.DEFAULT_TEST_FIELD, "michel"));
    q.setDatatype("http://www.w3.org/2001/XMLSchema#string");

    assertEquals(q, parser.parse("michel~", SirenTestCase.DEFAULT_TEST_FIELD));
  }

  @Test
  public void testPrefixQuery() throws Exception {
    ConciseKeywordQueryParser parser = new ConciseKeywordQueryParser();
    parser.setAttribute("aaa");

    NodePrefixQuery q = new NodePrefixQuery(new Term(SirenTestCase.DEFAULT_TEST_FIELD, "aaa:lit"));
    q.setDatatype("http://www.w3.org/2001/XMLSchema#string");

    assertEquals(q, parser.parse("lit*", SirenTestCase.DEFAULT_TEST_FIELD));

    // with no attribute defined

    parser = new ConciseKeywordQueryParser();

    q = new NodePrefixQuery(new Term(SirenTestCase.DEFAULT_TEST_FIELD, "lit"));
    q.setDatatype("http://www.w3.org/2001/XMLSchema#string");

    assertEquals(q, parser.parse("lit*", SirenTestCase.DEFAULT_TEST_FIELD));
  }

  @Test
  public void testNumericQuery() throws Exception {
    ConciseKeywordQueryParser parser = new ConciseKeywordQueryParser();
    parser.setAttribute("aaa");

    final Map<String, Analyzer> datatypes = new HashMap<String, Analyzer>();
    datatypes.put("int", new IntNumericAnalyzer(4));
    parser.setDatatypeAnalyzers(datatypes);

    DatatypedNodeQuery q = ConciseNodeNumericRangeQuery.newIntRange(SirenTestCase.DEFAULT_TEST_FIELD, "aaa", 4,
        50, 100, true, true);
    q.setDatatype("int");

    assertEquals(q, parser.parse("int([50 TO 100])", SirenTestCase.DEFAULT_TEST_FIELD));

    // with no attribute defined

    parser = new ConciseKeywordQueryParser();
    parser.setDatatypeAnalyzers(datatypes);

    q = NodeNumericRangeQuery.newIntRange(SirenTestCase.DEFAULT_TEST_FIELD, 4, 50, 100, true, true);
    q.setDatatype("int");

    assertEquals(q, parser.parse("int([50 TO 100])", SirenTestCase.DEFAULT_TEST_FIELD));
  }

}

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

package com.sindicetech.siren.search.node;

import static com.sindicetech.siren.analysis.MockSirenToken.node;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.BooleanClauseBuilder.must;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.BooleanClauseBuilder.not;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeBooleanQueryBuilder.nbq;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodePhraseQueryBuilder.npq;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;

import com.sindicetech.siren.index.DocsAndNodesIterator;
import com.sindicetech.siren.index.codecs.RandomSirenCodec.PostingsFormatType;
import com.sindicetech.siren.search.AbstractTestSirenScorer;
import com.sindicetech.siren.search.node.NodeScorer;

public class TestNodeReqExclScorer extends AbstractTestSirenScorer {

  @Override
  protected void configure() throws IOException {
    this.setAnalyzer(AnalyzerType.TUPLE);
    this.setPostingsFormat(PostingsFormatType.RANDOM);
  }

  @Test
  public void testNextCandidateDocument() throws Exception {
    this.addDocuments(
      "\"aaa bbb\" \"aaa ccc\" . \"aaa bbb ccc\" \"bbb ccc\" . ",
      "\"aaa\" \"aaa bbb\" . "
    );

    final NodeScorer scorer = this.getScorer(
      nbq(must("aaa"), not("bbb"))
    );

    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertEquals(node(-1), scorer.node());
    assertTrue(scorer.nextNode());
    assertEquals(node(0,1), scorer.node());
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());

    assertTrue(scorer.nextCandidateDocument());
    assertEquals(1, scorer.doc());
    assertEquals(node(-1), scorer.node());
    assertTrue(scorer.nextNode());
    assertEquals(node(0,0), scorer.node());
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());

    assertEndOfStream(scorer);
  }

  @Test
  public void testScore() throws Exception {
    this.addDocuments(
      "\"aaa bbb\" \"aaa ccc\" . \"aaa bbb ccc\" \"bbb ccc\" . ",
      "\"aaa\" \"aaa bbb\" . "
    );

    final NodeScorer scorer = this.getScorer(
      nbq(must("aaa"), not("bbb"))
    );

    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertEquals(node(-1), scorer.node());
    assertTrue(scorer.nextNode());
    assertEquals(node(0,1), scorer.node());
    final float d0score01 = scorer.scoreInNode();
    assertTrue(d0score01 + " > " + 0, d0score01 > 0);
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());

    assertTrue(scorer.nextCandidateDocument());
    assertEquals(1, scorer.doc());
    assertEquals(node(-1), scorer.node());
    assertTrue(scorer.nextNode());
    assertEquals(node(0,0), scorer.node());
    final float d1score00 = scorer.scoreInNode();
    assertTrue(d1score00 + " > " + 0, d1score00 > 0);
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());

    assertEndOfStream(scorer);
  }

  @Test
  public void testNextWithPhraseExclusion1()
  throws Exception {
    this.addDocuments("\"aaa bbb ccc\" \"aaa ccc\" . \"aaa bbb ccc ddd\" \"bbb aaa ccc ddd\" . ",
                      "\"aaa bbb ccc ccc ddd\" \"aaa bbb ddd ddd ccc\" . ",
                      "\"aaa bbb aaa bbb ccc ddd\" \"aaa bbb ddd ccc ddd ccc ddd\" . ");

    final NodeScorer scorer = this.getScorer(
      nbq(must(npq("aaa", "bbb")), not(npq("ccc", "ddd")))
    );

    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertEquals(node(-1), scorer.node());
    assertTrue(scorer.nextNode());
    assertEquals(node(0,0), scorer.node());
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());

    assertTrue(scorer.nextCandidateDocument());
    assertEquals(1, scorer.doc());
    assertEquals(node(-1), scorer.node());
    assertTrue(scorer.nextNode());
    assertEquals(node(0,1), scorer.node());
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());

    assertTrue(scorer.nextCandidateDocument());
    assertEquals(2, scorer.doc());
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());

    assertEndOfStream(scorer);
  }

  @Test
  public void testNextWithPhraseExclusion2()
  throws Exception {
    this.addDocument("\"aaa bbb ccc\" . \"ccc aaa bbb\" . ");

    final NodeScorer scorer = this.getScorer(
      nbq(must(npq("aaa", "bbb")), not(npq("bbb", "ccc")))
    );

    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertEquals(node(-1), scorer.node());
    assertTrue(scorer.nextNode());
    assertEquals(node(1,0), scorer.node());
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());

    assertEndOfStream(scorer);
  }

  @Test
  public void testNextNodeWithExhaustedProhibitedScorer() throws Exception {
    this.addDocuments(
      "\"aaa bbb\" \"aaa ccc\" . \"aaa bbb ccc\" \"bbb ccc\" . ",
      "\"aaa\" \"aaa bbb\" . "
    );

    final NodeScorer scorer = this.getScorer(
      nbq(must("aaa"), not("ccc"))
    );

    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertEquals(node(-1), scorer.node());
    assertTrue(scorer.nextNode());
    assertEquals(node(0,0), scorer.node());
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());

    assertTrue(scorer.nextCandidateDocument());
    assertEquals(1, scorer.doc());
    assertEquals(node(-1), scorer.node());
    assertTrue(scorer.nextNode());
    assertEquals(node(0,0), scorer.node());

    // here, the prohibited scorer should be set to null (exhausted), let see
    // if there is a null pointer exception somewhere
    assertTrue(scorer.nextNode());
    assertEquals(1, scorer.doc());
    assertEquals(node(0,1), scorer.node());
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());

    assertEndOfStream(scorer);
  }

  @Test
  public void testSkipToCandidate() throws Exception {
    final ArrayList<String> docs = new ArrayList<String>();
    for (int i = 0; i < 32; i++) {
      docs.add("\"aaa bbb\" \"aaa ccc\" . \"aaa bbb ccc\" \"bbb ccc\" \"aaa aaa\". ");
      docs.add("\"aaa bbb aaa\" . \"aaa ccc bbb\" . ");
    }
    this.addDocuments(docs);

    final NodeScorer scorer = this.getScorer(
      nbq(must("aaa"), not("bbb"))
    );

    assertTrue(scorer.skipToCandidate(16));
    assertEquals(16, scorer.doc());
    assertEquals(node(-1), scorer.node());
    assertTrue(scorer.nextNode());
    assertEquals(node(0,1), scorer.node());
    assertTrue(scorer.nextNode());
    assertEquals(node(1,2), scorer.node());
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());

    // Next candidate document should not contain any matching node
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(17, scorer.doc());
    assertEquals(node(-1), scorer.node());
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());

    assertTrue(scorer.skipToCandidate(40));
    assertEquals(40, scorer.doc());
    assertEquals(node(-1), scorer.node());
    assertTrue(scorer.nextNode());
    assertEquals(node(0,1), scorer.node());
    assertTrue(scorer.nextNode());
    assertEquals(node(1,2), scorer.node());
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());

    assertFalse(scorer.skipToCandidate(65));
    assertEndOfStream(scorer);
  }

}

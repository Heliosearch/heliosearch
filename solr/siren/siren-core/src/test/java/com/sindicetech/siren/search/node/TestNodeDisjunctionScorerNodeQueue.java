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
import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeTermQueryBuilder.ntq;

import java.io.IOException;

import org.apache.lucene.index.CorruptIndexException;
import org.junit.Test;

import com.sindicetech.siren.index.DocsAndNodesIterator;
import com.sindicetech.siren.index.codecs.RandomSirenCodec.PostingsFormatType;
import com.sindicetech.siren.search.AbstractTestSirenScorer;
import com.sindicetech.siren.search.node.NodeDisjunctionScorerQueue;
import com.sindicetech.siren.search.node.NodeScorer;

public class TestNodeDisjunctionScorerNodeQueue extends AbstractTestSirenScorer {

  @Override
  protected void configure() throws IOException {
    this.setAnalyzer(AnalyzerType.TUPLE);
    this.setPostingsFormat(PostingsFormatType.RANDOM);
  }

  /**
   * Test method for {@link com.sindicetech.siren.search.node.NodeDisjunctionScorerQueue#top()}.
   * @throws IOException
   * @throws CorruptIndexException
   */
  @Test
  public void testTop() throws IOException {
    this.addDocument("\"term1\" . ");
    this.addDocument("\"term2\" . ");
    this.addDocument("\"term3\" .  \"term4\" . ");

    final NodeDisjunctionScorerQueue q = new NodeDisjunctionScorerQueue(5);

    final NodeScorer s1 = this.getScorer(ntq("term1"));
    final NodeScorer s2 = this.getScorer(ntq("term2"));
    final NodeScorer s3 = this.getScorer(ntq("term3"));
    final NodeScorer s4 = this.getScorer(ntq("term4"));

    q.put(s3);
    assertSame(s3, q.top());
    q.put(s2);
    assertSame(s2, q.top()); // s2 should be the least scorer
    q.put(s4);
    assertSame(s2, q.top());
    q.put(s1);
    assertSame(s1, q.top()); // s1 should be the least scorer
  }

  @Test
  public void testNextCandidateDocumentAndAdjustElsePop() throws IOException {
    this.addDocument("\"term1\" \"term2\" . \"term3\" .  \"term4\" . ");
    this.addDocument("\"term5\" \"term2\" . \"term3\" .  ");

    final NodeDisjunctionScorerQueue q = new NodeDisjunctionScorerQueue(4);
    q.put(this.getScorer(ntq("term2")));
    q.put(this.getScorer(ntq("term3")));
    q.put(this.getScorer(ntq("term4")));
    q.put(this.getScorer(ntq("term5")));

    assertEquals(0, q.doc());
    assertEquals(4, q.size());
    assertTrue(q.nextCandidateDocumentAndAdjustElsePop());
    assertEquals(1, q.doc());
    assertEquals(3, q.size()); // term4 scorer should have been removed
    assertFalse(q.nextCandidateDocumentAndAdjustElsePop());

    assertEquals(DocsAndNodesIterator.NO_MORE_DOC, q.doc());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, q.node());
  }

  @Test
  public void testNextNodeAndAdjust() throws IOException {
    this.addDocument("\"term1\" \"term2\" . \"term3\" .  \"term4\" . ");
    this.addDocument("\"term2\" \"term3\" . \"term5\" .  ");
    this.addDocument("\"term2\" \"term1\" . \"term5\" .  ");

    final NodeDisjunctionScorerQueue q = new NodeDisjunctionScorerQueue(2);
    q.put(this.getScorer(ntq("term1")));
    q.put(this.getScorer(ntq("term5")));

    assertEquals(0, q.doc());
    assertTrue(q.nextNodeAndAdjust());
    assertEquals(node(0,0), q.node());
    assertFalse(q.nextNodeAndAdjust());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, q.node());

    assertTrue(q.nextCandidateDocumentAndAdjustElsePop());
    assertEquals(1, q.doc());
    assertTrue(q.nextNodeAndAdjust());
    assertEquals(node(1,0), q.node());
    assertFalse(q.nextNodeAndAdjust());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, q.node());

    assertTrue(q.nextCandidateDocumentAndAdjustElsePop());
    assertEquals(2, q.doc());
    assertTrue(q.nextNodeAndAdjust());
    assertEquals(node(0,1), q.node());
    assertTrue(q.nextNodeAndAdjust());
    assertEquals(node(1,0), q.node());
    assertFalse(q.nextNodeAndAdjust());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, q.node());

    assertFalse(q.nextCandidateDocumentAndAdjustElsePop());

    assertEquals(DocsAndNodesIterator.NO_MORE_DOC, q.doc());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, q.node());
  }

  @Test
  public void testNrMatches() throws IOException {
    this.addDocument("\"term1\" \"term2\" . \"term3\" .  \"term4\" . ");
    this.addDocument("\"term2\" \"term3\" . \"term5\" .  ");
    this.addDocument("\"term2\" \"term1 term5\" .  ");

    final NodeDisjunctionScorerQueue q = new NodeDisjunctionScorerQueue(2);
    q.put(this.getScorer(ntq("term1")));
    q.put(this.getScorer(ntq("term5")));

    assertEquals(0, q.doc());
    assertTrue(q.nextNodeAndAdjust());
    q.countAndSumMatchers();
    // there must be 1 matchers for node {0,0}
    assertEquals(1, q.nrMatchersInNode());

    assertTrue(q.nextCandidateDocumentAndAdjustElsePop());
    assertEquals(1, q.doc());
    assertTrue(q.nextNodeAndAdjust());
    q.countAndSumMatchers();
    // there must be 1 matchers for node {1,0}
    assertEquals(1, q.nrMatchersInNode());

    assertTrue(q.nextCandidateDocumentAndAdjustElsePop());
    assertEquals(2, q.doc());
    assertTrue(q.nextNodeAndAdjust());
    q.countAndSumMatchers();
    // there must be 2 matchers for node {0,1}
    assertEquals(2, q.nrMatchersInNode());
  }

  @Test
  public void testScoreSum() throws IOException {
    this.addDocument("\"term1 term2 term3\" .  \"term4\" . ");

    final NodeScorer s1 = this.getScorer(ntq("term1"));
    final NodeScorer s2 = this.getScorer(ntq("term2"));
    final NodeScorer s3 = this.getScorer(ntq("term3"));

    final NodeDisjunctionScorerQueue q = new NodeDisjunctionScorerQueue(3);
    q.put(s1);
    q.put(s2);
    q.put(s3);

    assertEquals(0, q.doc());
    assertTrue(q.nextNodeAndAdjust());
    q.countAndSumMatchers();
    // there must be 3 matchers
    assertEquals(3, q.nrMatchersInNode());
    final float scoreInNode = q.scoreInNode();
    assertEquals(s1.scoreInNode() + s2.scoreInNode() + s3.scoreInNode(), scoreInNode, 0f);
    assertTrue(scoreInNode != 0);
  }

  @Test
  public void testskipToCandidateAndAdjustElsePop() throws IOException {
    this.addDocument("\"term1\" \"term2\" . ");
    this.addDocument("\"term3\" .  \"term1\" . ");
    this.addDocument("\"term2\" \"term3\" . ");
    this.addDocument("\"term3\" .  \"term1\" . ");
    this.addDocument("\"term3\" .  \"term3\" . ");

    final NodeDisjunctionScorerQueue q = new NodeDisjunctionScorerQueue(3);
    q.put(this.getScorer(ntq("term1")));
    q.put(this.getScorer(ntq("term2")));
    q.put(this.getScorer(ntq("term3")));

    assertEquals(0, q.doc());
    assertTrue(q.skipToCandidateAndAdjustElsePop(3));
    assertEquals(3, q.doc());
    assertEquals(2, q.size()); // term2 should have been removed
    assertTrue(q.nextNodeAndAdjust());
    assertEquals(node(0,0), q.node());
    assertTrue(q.nextNodeAndAdjust());
    assertEquals(node(1,0), q.node());
    assertFalse(q.nextNodeAndAdjust());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, q.node());

    assertTrue(q.skipToCandidateAndAdjustElsePop(2));
    assertEquals(3, q.doc()); // queue should have not moved
    q.skipToCandidateAndAdjustElsePop(3);
    assertEquals(3, q.doc()); // queue should have not moved
    assertTrue(q.skipToCandidateAndAdjustElsePop(4));
    assertEquals(4, q.doc());
    assertEquals(1, q.size()); // term1 should have been removed
    assertTrue(q.nextNodeAndAdjust());
    assertEquals(node(0,0), q.node());
    assertTrue(q.nextNodeAndAdjust());
    assertEquals(node(1,0), q.node());
    assertFalse(q.nextNodeAndAdjust());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, q.node());

    assertFalse(q.skipToCandidateAndAdjustElsePop(7));
    assertEquals(DocsAndNodesIterator.NO_MORE_DOC, q.doc());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, q.node());
  }

  @Test
  public void testNextNodeHeapTraversal() throws IOException {
    this.addDocument("\"term1 term3\" \"term5 term2\" . \"term1 term3\" .  \"term5 term4 term3\" . ");

    final NodeDisjunctionScorerQueue q = new NodeDisjunctionScorerQueue(4);
    q.put(this.getScorer(ntq("term2")));
    q.put(this.getScorer(ntq("term3")));
    q.put(this.getScorer(ntq("term4")));
    q.put(this.getScorer(ntq("term5")));

    // test if the heap traversal is done properly.
    for (int i = 0; i < 4; i++) {
      q.nextNodeAndAdjust();
    }
    assertFalse(q.nextNodeAndAdjust());
  }

}

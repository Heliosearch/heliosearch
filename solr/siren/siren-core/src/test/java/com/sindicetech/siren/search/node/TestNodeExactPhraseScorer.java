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

import static com.sindicetech.siren.analysis.MockSirenDocument.doc;
import static com.sindicetech.siren.analysis.MockSirenToken.node;
import static com.sindicetech.siren.analysis.MockSirenToken.token;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodePhraseQueryBuilder.npq;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;

import com.sindicetech.siren.analysis.AnyURIAnalyzer;
import com.sindicetech.siren.analysis.TupleAnalyzer;
import com.sindicetech.siren.analysis.AnyURIAnalyzer.URINormalisation;
import com.sindicetech.siren.index.codecs.RandomSirenCodec.PostingsFormatType;
import com.sindicetech.siren.search.AbstractTestSirenScorer;
import com.sindicetech.siren.search.node.NodePhraseScorer;
import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.util.XSDDatatype;

public class TestNodeExactPhraseScorer extends AbstractTestSirenScorer {

  @Override
  protected void configure() throws IOException {
    this.setAnalyzer(AnalyzerType.TUPLE);
    // TODO: remove when TupleAnalyzer is no more used
    final AnyURIAnalyzer uriAnalyzer = new AnyURIAnalyzer(TEST_VERSION_CURRENT);
    uriAnalyzer.setUriNormalisation(URINormalisation.FULL);
    ((TupleAnalyzer) analyzer).registerDatatype(XSDDatatype.XSD_ANY_URI.toCharArray(), uriAnalyzer);
    this.setPostingsFormat(PostingsFormatType.RANDOM);
  }

  /**
   * Test exact phrase scorer: should not match two words in separate nodes
   */
  @Test
  public void testEmptyResult1() throws Exception {
    this.addDocument("\"word1 word2 word3\" \"word4 word5\" . ");

    final NodePhraseScorer scorer = (NodePhraseScorer) this.getScorer(npq("word1", "word4"));
    assertTrue(scorer.nextCandidateDocument());
    assertFalse(scorer.nextNode());
  }

  /**
   * Test exact phrase scorer: should not match phrase with a gap of 1 between
   * the two phrase query terms
   */
  @Test
  public void testEmptyResult2() throws Exception {
    this.addDocument("\"word1 word2 word3\" \"word4 word5\" . ");

    final NodePhraseScorer scorer = (NodePhraseScorer) this.getScorer(npq("word4", "", "word5"));
    assertTrue(scorer.nextCandidateDocument());
    assertFalse(scorer.nextNode());
  }

  @Test
  public void testNodeConstraint() throws Exception {
    this.addDocument("\"word1 word2 word3\" \"word4 word5\" . ");

    NodePhraseScorer scorer = (NodePhraseScorer) this.getScorer(npq("word4", "word5"));
    assertTrue(scorer.nextCandidateDocument());
    assertTrue(scorer.nextNode());

    scorer = (NodePhraseScorer) this.getScorer(npq("word4", "word5").level(2));
    assertTrue(scorer.nextCandidateDocument());
    assertTrue(scorer.nextNode());

    scorer = (NodePhraseScorer) this.getScorer(npq("word4", "word5").level(1));
    assertTrue(scorer.nextCandidateDocument());
    assertFalse(scorer.nextNode());

    scorer = (NodePhraseScorer) this.getScorer(npq("word4", "word5").bound(0,0));
    assertTrue(scorer.nextCandidateDocument());
    assertFalse(scorer.nextNode());

    scorer = (NodePhraseScorer) this.getScorer(npq("word4", "word5").bound(0,1));
    assertTrue(scorer.nextCandidateDocument());
    assertTrue(scorer.nextNode());
  }

  @Test
  public void testMultipleOccurrences() throws Exception {
    this.addDocument("<http://renaud.delbru.fr/> \"renaud delbru delbru renaud renaud delbru\" . ");

    NodeQuery q = npq("renaud", "delbru").getQuery();
    NodePhraseScorer scorer = (NodePhraseScorer) this.getScorer(q);

    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(0,0), scorer.node());
    assertEquals(1.0f, scorer.freqInNode(), 0);
    assertTrue(scorer.nextNode());
    assertEquals(node(0,1), scorer.node());
    assertEquals(2.0f, scorer.freqInNode(), 0);

    assertFalse(scorer.nextNode());
    assertFalse(scorer.nextCandidateDocument());

    q = npq("renaud", "", "delbru").getQuery();
    scorer = (NodePhraseScorer) this.getScorer(q);

    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(0,1), scorer.node());
    assertEquals(2.0f, scorer.freqInNode(), 0);

    assertFalse(scorer.nextNode());
    assertFalse(scorer.nextCandidateDocument());
  }

  @Test
  public void testSkipToCandidate() throws Exception {
    final ArrayList<String> docs = new ArrayList<String>();
    for (int i = 0; i < 32; i++) {
      docs.add("<http://renaud.delbru.fr/> . ");
    }
    this.addDocuments(docs);

    final NodePhraseScorer scorer = (NodePhraseScorer) this.getScorer(npq("renaud", "delbru"));
    assertTrue(scorer.skipToCandidate(16));
    assertEquals(16, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(0,0), scorer.node());
  }

  @Test
  public void testSkipToCandidateNext() throws Exception {
    final ArrayList<String> docs = new ArrayList<String>();
    for (int i = 0; i < 32; i++)
      docs.add("<http://renaud.delbru.fr/> . ");
    this.addDocuments(docs);

    final NodePhraseScorer scorer = (NodePhraseScorer) this.getScorer(npq("renaud", "delbru"));
    assertTrue(scorer.nextCandidateDocument());
    assertTrue(scorer.skipToCandidate(16));
    assertEquals(16, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(0,0), scorer.node());
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(17, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(0,0), scorer.node());
  }

//  @Test(expected=InvalidCallException.class)
//  public void testInvalidScoreCall() throws IOException {
//    _helper.addDocument("\"Renaud Delbru\" . ");
//
//    final Term t1 = new Term(QueryTestingHelper.DEFAULT_FIELD, "renaud");
//    final Term t2 = new Term(QueryTestingHelper.DEFAULT_FIELD, "delbru");
//    final NodePhraseQuery query = new NodePhraseQuery();
//    query.add(t1); query.add(t2);
//    final Weight w = query.createWeight(_helper.getIndexSearcher());
//
//    final IndexReader reader = _helper.getIndexReader();
//    final DocsAndPositionsEnum[] tps = new DocsAndPositionsEnum[2];
//    tps[0] = MultiFields.getTermPositionsEnum(reader, MultiFields.getLiveDocs(reader), t1.field(), t1.bytes());
//    tps[1] = MultiFields.getTermPositionsEnum(reader, MultiFields.getLiveDocs(reader), t2.field(), t2.bytes());
//
//    final NodePhraseScorer scorer = new NodeExactPhraseScorer(w, tps, new int[] {0, 1},
//      _helper.getIndexSearcher().getSimilarityProvider().get(QueryTestingHelper.DEFAULT_FIELD),
//      MultiNorms.norms(reader, QueryTestingHelper.DEFAULT_FIELD));
//    assertNotNull("ts is null and it shouldn't be", scorer);
//
//    // Invalid call
//    scorer.score();
//  }
//
//  @Test
//  public void testScore() throws IOException {
//    _helper.addDocument("\"Renaud Delbru\" . <http://renaud.delbru.fr> . ");
//
//    final Term t1 = new Term(QueryTestingHelper.DEFAULT_FIELD, "renaud");
//    final Term t2 = new Term(QueryTestingHelper.DEFAULT_FIELD, "delbru");
//    final NodePhraseQuery query = new NodePhraseQuery();
//    query.add(t1); query.add(t2);
//
//    final IndexReader reader = _helper.getIndexReader();
//    final DocsAndPositionsEnum[] tps = new DocsAndPositionsEnum[2];
//    tps[0] = MultiFields.getTermPositionsEnum(reader, MultiFields.getLiveDocs(reader), t1.field(), t1.bytes());
//    tps[1] = MultiFields.getTermPositionsEnum(reader, MultiFields.getLiveDocs(reader), t2.field(), t2.bytes());
//
//    final NodePhraseScorer scorer = new NodeExactPhraseScorer(
//      new ConstantWeight(), tps, new int[] {0, 1},
//      _helper.getIndexSearcher().getSimilarityProvider().get(QueryTestingHelper.DEFAULT_FIELD),
//      MultiNorms.norms(reader, QueryTestingHelper.DEFAULT_FIELD));
//    assertNotNull("ts is null and it shouldn't be", scorer);
//
//    assertFalse("no doc returned", scorer.nextDocument() == DocIdSetIterator.NO_MORE_DOCS);
//    assertEquals(0, scorer.doc());
//    assertEquals(0.70, scorer.score(), 0.01);
//  }

  /**
   * GH-70: Siren10PostingsReader does not properly compute pending positions
   * Siren10PostingsReader was not properly decoding termFreqInNode and counting pending positions when
   * a node was skipped without reading its termFreqInNode.
   */
  @Test
  public void testPositionDecodingAfterSkippingNode() throws Exception {
    this.setAnalyzer(AnalyzerType.MOCK);

    this.addDocuments(
      doc(token("person", node(0, 0), 1), token("1", node(0, 0), 1),
          token("person", node(0, 2, 0), 2), token("2", node(0, 2, 0), 1))
    );

    // Adding the node level 3 constraint will skip the first node [0,0]
    final NodePhraseScorer scorer = (NodePhraseScorer) this.getScorer(npq("person", "2").level(3));
    assertTrue(scorer.nextCandidateDocument());
    assertTrue(scorer.nextNode());
  }

}

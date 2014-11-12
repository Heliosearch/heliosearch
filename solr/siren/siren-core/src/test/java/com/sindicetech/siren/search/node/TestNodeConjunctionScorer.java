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
import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeBooleanQueryBuilder.nbq;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;

import com.sindicetech.siren.analysis.AnyURIAnalyzer;
import com.sindicetech.siren.analysis.TupleAnalyzer;
import com.sindicetech.siren.analysis.AnyURIAnalyzer.URINormalisation;
import com.sindicetech.siren.index.DocsAndNodesIterator;
import com.sindicetech.siren.index.codecs.RandomSirenCodec.PostingsFormatType;
import com.sindicetech.siren.search.AbstractTestSirenScorer;
import com.sindicetech.siren.search.node.LuceneProxyNodeScorer;
import com.sindicetech.siren.search.node.NodeScorer;
import com.sindicetech.siren.util.XSDDatatype;

public class TestNodeConjunctionScorer extends AbstractTestSirenScorer {

  @Override
  protected void configure() throws IOException {
    this.setAnalyzer(AnalyzerType.TUPLE);
    // TODO: remove when TupleAnalyzer is no more used
    final AnyURIAnalyzer uriAnalyzer = new AnyURIAnalyzer(TEST_VERSION_CURRENT);
    uriAnalyzer.setUriNormalisation(URINormalisation.FULL);
    ((TupleAnalyzer) analyzer).registerDatatype(XSDDatatype.XSD_ANY_URI.toCharArray(), uriAnalyzer);
    this.setPostingsFormat(PostingsFormatType.RANDOM);
  }

  @Test
  public void testNextWithTermConjunction() throws Exception {
    this.addDocuments(new String[] { "<http://renaud.delbru.fr/> . ",
      "<http://sindice.com/test/name> \"Renaud Delbru\" . ",
      "<http://sindice.com/test/type> <http://sindice.com/test/Person> . " +
      "<http://sindice.com/test/name> \"Renaud Delbru\" . " });

    final NodeScorer scorer = this.getScorer(
      nbq(must("renaud"), must("renaud"))
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
    assertEquals(node(-1), scorer.node());
    assertTrue(scorer.nextNode());
    assertEquals(node(1,1), scorer.node());
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());

    assertEndOfStream(scorer);
  }

  @Test
  public void testNoNode() throws IOException {
    this.addDocument("\"eee\" . \"ddd\" . ");

    final NodeScorer scorer = this.getScorer(
      nbq(must("ddd"), must("eee"))
    );

    assertTrue(scorer.nextCandidateDocument());
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());

    assertEndOfStream(scorer);
  }

  @Test
  public void testNoNextCandidate() throws IOException {
    this.addDocument("\"eee\" . \"ddd\" . ");
    this.addDocument("\"eee\" . \"fff\" . ");

    final NodeScorer scorer = this.getScorer(
      nbq(must("ddd"), must("fff"))
    );

    assertEndOfStream(scorer);
  }

// TODO: To update when phrase query implemented
//  @Test
//  public void testNextWithPhraseConjunction()
//  throws Exception {
//    this.deleteAll(writer);
//    this.addDocumentsWithIterator(new String[] { "\"aaa bbb aaa\". ",
//      "\"aaa bbb aba\" \"aaa ccc bbb aaa\" . ",
//      "\"aaa bbb ccc\" \"aaa ccc aaa aaa ccc\" . " +
//      "\" bbb ccc aaa \" \"aaa bbb bbb ccc aaa ccc\" . "});
//
//    final NodeBooleanScorer scorer =
//      this.getConjunctionScorer(new String[][] {{"aaa", "bbb"}, {"aaa", "ccc"}});
//
//    assertFalse(scorer.nextDocument() == DocIdSetIterator.NO_MORE_DOCS);
//    assertEquals(2, scorer.doc());
//    assertEquals(1, scorer.node()[0]);
//    assertEquals(1, scorer.node()[1]);
//    assertTrue(scorer.nextDocument() == DocIdSetIterator.NO_MORE_DOCS);
//  }

  @Test
  public void testSkipToCandidate() throws Exception {
    final ArrayList<String> docs = new ArrayList<String>();
    for (int i = 0; i < 32; i++) {
      docs.add("<http://sindice.com/test/name> \"Renaud Delbru\" . ");
      docs.add("<http://sindice.com/test/type> <http://sindice.com/test/Person> . ");
    }
    this.addDocuments(docs);

    final NodeScorer scorer = this.getScorer(
      nbq(must("renaud"), must("delbru"))
    );

    assertTrue(scorer.skipToCandidate(16));
    assertEquals(16, scorer.doc());
    assertEquals(node(-1), scorer.node());
    assertTrue(scorer.nextNode());
    assertEquals(node(0,1), scorer.node());
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());

    assertTrue(scorer.skipToCandidate(41)); // should jump to next candidate doc 42
    assertEquals(42, scorer.doc());
    assertEquals(node(-1), scorer.node());

    assertTrue(scorer.skipToCandidate(42)); // should stay at the same position
    assertEquals(42, scorer.doc());
    assertEquals(node(-1), scorer.node());
    assertTrue(scorer.nextNode());
    assertEquals(node(0,1), scorer.node());
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());

    assertFalse(scorer.skipToCandidate(75));
    assertEndOfStream(scorer);
  }

  /**
   * The score increases, even though the frequency of each term remains the same.
   * This is due to the length of the document which gets longer.
   */
  @Test
  public void testScoreWithTermConjunction()
  throws Exception {
    final String[] docs = new String[] {
      "<http://renaud.delbru.fr/> . ",
      "<http://sindice.com/test/name> \"Renaud Delbru\" . ",
      "<http://sindice.com/test/type> <http://sindice.com/test/Person> . " +
          "<http://sindice.com/test/name> \"Renaud Delbru\" . ",
      "<http://sindice.com/test/type> <http://sindice.com/test/Person> . " +
          "<http://sindice.com/test/homepage> <http://renaud.delbru.fr/> . " +
          "<http://sindice.com/test/name> \"Renaud Delbru\" ."
    };
    this.addDocuments(docs);

    final LuceneProxyNodeScorer scorer = new LuceneProxyNodeScorer(this.getScorer(nbq(must("renaud"), must("delbru"))));

    float lastLastScore = 0;
    float lastScore = 0;

    assertTrue(scorer.nextDoc() != DocsAndNodesIterator.NO_MORE_DOC);
    lastLastScore = scorer.score();

    assertTrue(scorer.nextDoc() != DocsAndNodesIterator.NO_MORE_DOC);
    lastScore = scorer.score();
    assertTrue("doc=" + scorer.docID() + " lastScore=" + lastLastScore + " score=" + lastScore, lastLastScore > lastScore);

    assertTrue(scorer.nextDoc() != DocsAndNodesIterator.NO_MORE_DOC);
    lastLastScore = lastScore;
    lastScore = scorer.score();
    assertTrue("lastScore=" + lastLastScore + " score=" + lastScore, lastLastScore > lastScore);
    lastLastScore = scorer.score();

    assertTrue(scorer.nextDoc() != DocsAndNodesIterator.NO_MORE_DOC);
    lastLastScore = lastScore;
    lastScore = scorer.score();
    // score() sums the score of both nodes
    assertTrue("lastScore=" + lastLastScore + " score=" + lastScore, lastLastScore < lastScore);

    assertFalse(scorer.nextDoc() != DocsAndNodesIterator.NO_MORE_DOC);
  }

}

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
import static com.sindicetech.siren.search.AbstractTestSirenScorer.BooleanClauseBuilder.should;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeBooleanQueryBuilder.nbq;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.TupleQueryBuilder.tuple;

import java.io.IOException;

import org.apache.lucene.index.CorruptIndexException;
import org.junit.Test;

import com.sindicetech.siren.analysis.AnyURIAnalyzer;
import com.sindicetech.siren.analysis.TupleAnalyzer;
import com.sindicetech.siren.analysis.AnyURIAnalyzer.URINormalisation;
import com.sindicetech.siren.index.codecs.RandomSirenCodec.PostingsFormatType;
import com.sindicetech.siren.search.AbstractTestSirenScorer;
import com.sindicetech.siren.search.node.NodeScorer;
import com.sindicetech.siren.util.XSDDatatype;

public class TestTupleScorer extends AbstractTestSirenScorer {

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
  public void testUnaryClause() throws IOException {
    this.addDocument("\"aaa ccc\" \"bbb ccc\" . \"aaa bbb\" \"ccc eee\" . ");

    NodeScorer scorer = this.getScorer(
      tuple().optional(nbq(must("aaa"), must("ccc")))
    );

    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(0), scorer.node());
    assertEndOfStream(scorer);

    scorer = this.getScorer(
      tuple().optional(nbq(must("aaa"), must("bbb")))
    );

    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(1), scorer.node());
    assertEndOfStream(scorer);

    scorer = this.getScorer(
      tuple().optional(nbq(must("aaa"), must("eee")))
    );

    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertFalse(scorer.nextNode());
    assertEndOfStream(scorer);
  }

  @Test
  public void testMoreThanOneClause() throws IOException {
    this.addDocument("\"aaa ccc\" \"bbb ccc\" . \"aaa bbb\" \"ccc eee\" . ");

    NodeScorer scorer = this.getScorer(
      tuple().with(nbq(must("aaa"), must("ccc")))
             .with(nbq(must("aaa"), must("bbb")))
    );

    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertFalse(scorer.nextNode());
    assertEndOfStream(scorer);

    scorer = this.getScorer(
      tuple().with(nbq(must("aaa"), must("ccc")))
             .with(nbq(must("bbb"), must("ccc")))
    );

    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(0), scorer.node());
    assertEndOfStream(scorer);
  }

  /**
   * <code>{+[ddd] +[eee]}</code>
   */
  @Test
  public void testMust() throws IOException {
    this.addDocument("\"eee\" . \"ddd\" . ");
    this.addDocument("\"bbb\" . \"ddd eee\" . ");

    final NodeScorer scorer = this.getScorer(
      tuple().with(nbq(should("ddd")))
             .with(nbq(should("eee")))
    );

    // first candidate document does not match
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertFalse(scorer.nextNode());
    // second candidate document is matching
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(1, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(1), scorer.node());
    assertEndOfStream(scorer);
  }

  /**
   * <code>{+[ddd] [eee]}</code>
   */
  @Test
  public void testMustShould() throws IOException {
    this.addDocument("\"eee\" \"ddd\" . ");
    this.addDocument("\"bbb\" . \"ddd\" . ");
    this.addDocument("\"bbb\" . \"eee\" . ");

    final NodeScorer scorer = this.getScorer(
      tuple().with(nbq(should("ddd")))
             .optional(nbq(should("eee")))
    );

    // first candidate is matching
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(0), scorer.node());
    // second candidate is matching
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(1, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(1), scorer.node());
    // third document is not a candidate
    assertFalse(scorer.nextCandidateDocument());

    assertEndOfStream(scorer);
  }

  /**
   * <code>{+[ddd] -[eee]}</code>
   */
  @Test
  public void testMustMustNot() throws IOException {
    this.addDocument("\"eee\" \"ddd aaa\" . ");
    this.addDocument("\"bbb\" \"ddd eee\" . ");
    this.addDocument("\"bbb\" \"ddd\" . ");

    final NodeScorer scorer = this.getScorer(
      tuple().with(nbq(should("ddd")))
             .without(nbq(should("eee")))
    );

    // first and second candidate documents do not match
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertFalse(scorer.nextNode());
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(1, scorer.doc());
    assertFalse(scorer.nextNode());
    // third candidate document matches
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(2, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(0), scorer.node());

    assertEndOfStream(scorer);
  }

  /**
   * <code>{[ddd] [eee]}</code>
   */
  @Test
  public void testShould() throws IOException {
    this.addDocument("\"eee\" \"ddd\" . ");
    this.addDocument("\"bbb\" \"ddd\" . ");

    final NodeScorer scorer = this.getScorer(
      tuple().optional(nbq(should("ddd")))
             .optional(nbq(should("eee")))
    );

    // the two documents match
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(0), scorer.node());
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(1, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(0), scorer.node());

    assertEndOfStream(scorer);
  }

  /**
   * <code>{[ddd] -[eee]}</code>
   */
  @Test
  public void testShouldMustNot() throws IOException {
    this.addDocument("\"eee\" . \"ddd\" . ");
    this.addDocument("\"bbb\" . \"ddd eee\" . ");

    final NodeScorer scorer = this.getScorer(
      tuple().optional(nbq(should("ddd")))
             .without(nbq(should("eee")))
    );

    // first document matches
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(1), scorer.node());

    // second candidate document does not match
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(1, scorer.doc());
    assertFalse(scorer.nextNode());

    assertEndOfStream(scorer);
  }

  @Test
  public void testTupleConstraintOneClause() throws IOException {
    this.addDocument("<aaa> <bbb> . <ccc> <ddd> . ");
    this.addDocument("<ccc> . <aaa> <bbb> <ddd> . ");

    final NodeScorer scorer = this.getScorer(
      tuple().with(nbq(must("ccc")))
             .bound(1, 1)
    );

    // first document matches
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(1), scorer.node());

    // second candidate document do not match
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(1, scorer.doc());
    assertFalse(scorer.nextNode());

    assertEndOfStream(scorer);
  }

  @Test
  public void testTupleConstraintTwoClauses() throws IOException {
    this.addDocument("<aaa> <bbb> . <ccc> <ddd> . ");
    this.addDocument("<ccc> <ddd> . <aaa> <bbb> <ddd> . ");

    final NodeScorer scorer = this.getScorer(
      tuple().with(nbq(must("ccc")).bound(0,0))
             .with(nbq(must("ddd")).bound(1,1))
             .bound(1, 1)
    );

    // first document matches
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(1), scorer.node());

    // second candidate document do not match
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(1, scorer.doc());
    assertFalse(scorer.nextNode());

    assertEndOfStream(scorer);
  }

  /**
   * Test conjunction with exhausted scorer.
   * The scorer of ddd got exhausted, and
   * {@link SirenCellConjunctionScorer#doNext()} was trying to retrieve the
   * entity id from the exhausted scorer.
   */
  @Test
  public void testConjunctionWithExhaustedScorer() throws IOException {
    this.addDocument("\"ccc\" . <aaa> \"ddd\" . ");
    this.addDocument("\"ccc\" . <aaa> \"ddd eee\" . ");

    final NodeScorer scorer = this.getScorer(
      tuple().with(nbq(must("aaa")).bound(0,0))
             .with(nbq(must("ddd"), not("eee")).bound(1,Integer.MAX_VALUE))
    );

    // first candidate document matches
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(1), scorer.node());

    // second candidate document do not match
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(1, scorer.doc());
    assertFalse(scorer.nextNode());

    assertEndOfStream(scorer);
  }

  @Test
  public void testMultiValuedPredicate() throws CorruptIndexException, IOException {
    this.addDocument("<aaa> \"ddd eee\" \"ddd ccc\" \"ccc eee\" \"eee bbb\" . ");
    this.addDocument("<aaa> \"ddd bbb\" \"ddd bbb\" \"eee bbb\" \"eee ccc\" . ");
    this.addDocument("<aaa> \"ddd ccc\" \"ddd bbb eee\" \"eee ccc bbb\" \"eee ccc\" . ");
    this.addDocument("<aaa> \"ddd eee\" \"ddd eee\" \"eee ccc bbb\" \"eee ccc\" . ");
    this.addDocument("<bbb> \"ddd eee\" \"ddd eee\" \"eee ccc ddd\" \"eee ccc\" . ");
    this.addDocument("<aaa> \"ddd eee\" \"ddd eee\" \"eee ccc bbb\" \"eee ccc\" . \n" +
        "<bbb> \"ddd ccc\" \"ddd bbb eee\" \"eee ccc bbb\" \"eee ccc\" .\n" +
        "<ccc> \"aaa eee ccc\" \"bbb eee ccc\" . ");

    final NodeScorer scorer = this.getScorer(
      tuple().with(nbq(must("aaa")).bound(0,0))
             .with(nbq(must("ddd"), must("ccc")).bound(1,Integer.MAX_VALUE))
    );

    // first candidate document matches
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(0), scorer.node());

    // second candidate document do not match
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(1, scorer.doc());
    assertFalse(scorer.nextNode());

    // third candidate document matches
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(2, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(0), scorer.node());

    // fourth candidate document do not match
    assertTrue(scorer.nextCandidateDocument());
    assertFalse(scorer.nextNode());

    // fifth candidate document should be skipped

    // sixth candidate document do not match
    assertTrue(scorer.nextCandidateDocument());
    assertFalse(scorer.nextNode());

    assertEndOfStream(scorer);
  }

  @Test
  public void testTuple2ReqCell1Excl() throws CorruptIndexException, IOException {
    this.addDocument("<aaa> <bbb> <ddd> <eee> . ");
    this.addDocument("<aaa> <ccc> <eee> . ");
    this.addDocument("<aaa> <ccc> <ddd> . ");
    this.addDocument("<aaa> <ccc> <eee> <ddd> . ");

    final NodeScorer scorer = this.getScorer(
      tuple().with(nbq(must("aaa")).bound(0,0))
             .with(nbq(must("eee")).bound(1,Integer.MAX_VALUE))
             .without(nbq(must("ddd")).bound(1,Integer.MAX_VALUE))
    );

    // first candidate document do not match
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(0, scorer.doc());
    assertFalse(scorer.nextNode());

    // second candidate document matches
    assertTrue(scorer.nextCandidateDocument());
    assertEquals(1, scorer.doc());
    assertTrue(scorer.nextNode());
    assertEquals(node(0), scorer.node());

    // third candidate document should be skipped

    // fourth candidate document do not match
    assertTrue(scorer.nextCandidateDocument());
    assertFalse(scorer.nextNode());

    assertEndOfStream(scorer);
  }

  @Test
  public void testMultiValuedPredicate2() throws CorruptIndexException, IOException {
    final String[] docs = new String[300];
    for (int i = 0; i < 100; i++) {
      docs[i * 3] = "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationTag> \"data data figure obtained\" \"belief tln parameters graphical\" \"incorrect rose proportions feature\" .";
      docs[i * 3 + 1] = "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationTag> \"statistical determining data ylx\" \"presented assumed mit factors\" \"jolla developed positive functions\" .";
      docs[i * 3 + 2] = "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationTag> \"data accuracy minutes applying\" \"focus perceive em parameterization\" \"yield learning separation rule\" .";
    }
    this.addDocuments(docs);

    final NodeScorer scorer = this.getScorer(
      tuple().with(nbq(must("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationtag")).bound(0,0))
             .with(nbq(must("data"), must("accuracy")).bound(1,Integer.MAX_VALUE))
    );

    for (int i = 0; i < 100; i++) {
      // first and second documents should be skipped

      // third candidate document matches
      assertTrue(scorer.nextCandidateDocument());
      assertTrue(scorer.nextNode());
      assertEquals(node(0), scorer.node());

    }

    assertEndOfStream(scorer);
  }

}

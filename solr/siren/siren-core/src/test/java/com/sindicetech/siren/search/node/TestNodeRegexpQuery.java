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

import com.sindicetech.siren.analysis.AnyURIAnalyzer;
import com.sindicetech.siren.analysis.TupleAnalyzer;
import com.sindicetech.siren.index.codecs.RandomSirenCodec.PostingsFormatType;
import com.sindicetech.siren.util.BasicSirenTestCase;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.automaton.*;

import java.io.IOException;
import java.util.Arrays;

import static com.sindicetech.siren.search.AbstractTestSirenScorer.dq;

/**
 * Code taken from {@link TestRegexpQuery} and adapted for SIREn.
 */
public class TestNodeRegexpQuery extends BasicSirenTestCase {

  @Override
  protected void configure() throws IOException {
    this.setAnalyzer(
      new TupleAnalyzer(TEST_VERSION_CURRENT,
        new WhitespaceAnalyzer(TEST_VERSION_CURRENT),
        new AnyURIAnalyzer(TEST_VERSION_CURRENT))
    );
    this.setPostingsFormat(PostingsFormatType.RANDOM);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.addDocument("\"the quick brown fox jumps over the lazy ??? dog 493432 49344\"");
  }

  private Term newTerm(final String value) {
    return new Term(DEFAULT_TEST_FIELD, value);
  }

  private int regexQueryNrHits(final String regex) throws IOException {
    final NodeRegexpQuery query = new NodeRegexpQuery(this.newTerm(regex));
    return searcher.search(dq(query), 5).totalHits;
  }

  public void testRegex1() throws IOException {
    assertEquals(1, this.regexQueryNrHits("q.[aeiou]c.*"));
  }

  public void testRegex2() throws IOException {
    assertEquals(0, this.regexQueryNrHits(".[aeiou]c.*"));
  }

  public void testRegex3() throws IOException {
    assertEquals(0, this.regexQueryNrHits("q.[aeiou]c"));
  }

  public void testNumericRange() throws IOException {
    assertEquals(1, this.regexQueryNrHits("<420000-600000>"));
    assertEquals(0, this.regexQueryNrHits("<493433-600000>"));
  }

  public void testRegexComplement() throws IOException {
    assertEquals(1, this.regexQueryNrHits("4934~[3]"));
    // not the empty lang, i.e. match all docs
    assertEquals(1, this.regexQueryNrHits("~#"));
  }

  public void testCustomProvider() throws IOException {
    final AutomatonProvider myProvider = new AutomatonProvider() {
      // automaton that matches quick or brown
      private final Automaton quickBrownAutomaton = Operations.union(Arrays
        .asList(Automata.makeString("quick"),
                Automata.makeString("brown"),
                Automata.makeString("bob")));

      public Automaton getAutomaton(final String name) {
        if (name.equals("quickBrown")) return quickBrownAutomaton;
        else return null;
      }
    };
    final NodeRegexpQuery query = new NodeRegexpQuery(this.newTerm("<quickBrown>"),
      RegExp.ALL, myProvider);
    assertEquals(1, searcher.search(dq(query), 5).totalHits);
  }

  /**
   * Test a corner case for backtracking: In this case the term dictionary has
   * 493432 followed by 49344. When backtracking from 49343... to 4934, its
   * necessary to test that 4934 itself is ok before trying to append more
   * characters.
   */
  public void testBacktracking() throws IOException {
    assertEquals(1, this.regexQueryNrHits("4934[314]"));
  }

}

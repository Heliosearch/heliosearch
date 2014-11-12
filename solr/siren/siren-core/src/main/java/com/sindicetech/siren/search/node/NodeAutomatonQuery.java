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

import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.ToStringUtils;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import java.io.IOException;

/**
 * A {@link MultiNodeTermQuery} that will match terms against a finite-state
 * machine.
 *
 * <p>
 *
 * This query will match documents that contain terms accepted by a given
 * finite-state machine. The automaton can be constructed with the
 * {@link org.apache.lucene.util.automaton} API. Alternatively, it can be
 * created from a regular expression with {@link RegexpQuery} or from
 * the standard Lucene wildcard syntax with {@link NodeWildcardQuery}.
 *
 * <p>
 *
 * When the query is executed, it will create an equivalent DFA of the
 * finite-state machine, and will enumerate the term dictionary in an
 * intelligent way to reduce the number of comparisons. For example: the regular
 * expression of <code>[dl]og?</code> will make approximately four comparisons:
 * do, dog, lo, and log.
 */
public class NodeAutomatonQuery extends MultiNodeTermQuery {

  /** the automaton to match index terms against */
  protected final Automaton automaton;
  protected final CompiledAutomaton compiled;
  /** term containing the field, and possibly some pattern structure */
  protected final Term term;

  /**
   * Create a new AutomatonQuery from an {@link Automaton}.
   *
   * @param term Term containing field and possibly some pattern structure. The
   *        term text is ignored.
   * @param automaton Automaton to run, terms that are accepted are considered a
   *        match.
   */
  public NodeAutomatonQuery(final Term term, final Automaton automaton) {
    super(term.field());
    this.term = term;
    this.automaton = automaton;
    this.compiled = new CompiledAutomaton(automaton);
  }

  @Override
  protected TermsEnum getTermsEnum(final Terms terms, final AttributeSource atts) throws IOException {
    return compiled.getTermsEnum(terms);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + compiled.hashCode();
    result = prime * result + ((term == null) ? 0 : term.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (this.getClass() != obj.getClass())
      return false;
    final NodeAutomatonQuery other = (NodeAutomatonQuery) obj;
    if (!compiled.equals(other.compiled))
      return false;
    if (term == null) {
      if (other.term != null)
        return false;
    } else if (!term.equals(other.term))
      return false;
    return true;
  }

  @Override
  public String toString(final String field) {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName());
    buffer.append(" {");
    buffer.append('\n');
    buffer.append(automaton.toString());
    buffer.append("}");
    buffer.append(ToStringUtils.boost(this.getBoost()));
    return buffer.toString();
  }
}


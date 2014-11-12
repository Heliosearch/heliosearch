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

package com.sindicetech.siren.search.spans;

import org.apache.lucene.util.IntsRef;

import java.io.IOException;
import java.util.List;

/**
 * A {@link com.sindicetech.siren.search.spans.Spans} that matches a boolean combination
 * {@link com.sindicetech.siren.search.spans.Spans}.
 *
 * <p>
 *
 * Uses {@link com.sindicetech.siren.search.spans.NearSpans}, {@link com.sindicetech.siren.search.spans.ReqOptSpans},
 * {@link com.sindicetech.siren.search.spans.NotSpans} and {@link com.sindicetech.siren.search.spans.OrSpans}.
 *
 * <p>
 *
 * Current known limitations:
 * <ul>
 * <li> Only the required clauses are subject to the order constraint. The optional and prohibited clauses will match
 * irrespective of the order.
 * </ul>
 */
class BooleanSpans extends Spans {

  protected final List<Spans> requiredSpans;
  protected final List<Spans> optionalSpans;
  protected final List<Spans> prohibitedSpans;

  protected final int slop;
  private final boolean inOrder;

  /**
   * The spans to which all scoring will be delegated, except for computing and
   * using the coordination factor.
   */
  protected Spans parentSpans = null;

  /**
   * Creates a {@link com.sindicetech.siren.search.spans.BooleanSpans} with the given lists of
   * required, prohibited and optional spans. In no required spans are added,
   * at least one of the optional spans will have to match during the search.
   *
   * @param required
   *          the list of required spans.
   * @param prohibited
   *          the list of prohibited spans.
   * @param optional
   *          the list of optional spans.
   * @param slop
   *          the maximum allowed slop.
   * @param inOrder
   *          specify if the required spans must be ordered.
   */
  public BooleanSpans(final List<Spans> required, final List<Spans> optional, final List<Spans> prohibited,
                      final int slop, final boolean inOrder)
  throws IOException {
    optionalSpans = optional;
    requiredSpans = required;
    prohibitedSpans = prohibited;

    this.slop = slop;
    this.inOrder = inOrder;

    parentSpans = this.buildSpans();
  }

  /**
   * Returns the scorer to be used for match counting and score summing. Uses
   * requiredSpans, optionalSpans and prohibitedSpans.
   */
  private Spans buildSpans() throws IOException {
    return (requiredSpans.size() == 0) ? this.buildSpansNoReq()
                                       : this.buildSpansSomeReq();
  }

  /**
   * No required scorers
   */
  private Spans buildSpansNoReq() throws IOException {
    Spans requiredSpans = null;
    if (optionalSpans.size() > 1) {
      requiredSpans = new OrSpans(optionalSpans);
    }
    else if (optionalSpans.size() == 1) {
      requiredSpans = optionalSpans.get(0);
    }
    return this.addProhibitedSpans(requiredSpans);
  }

  /**
   * At least one required scorer.
   */
  private Spans buildSpansSomeReq() throws IOException {
    final Spans conjunctionSpans = this.buildConjunctionSpans();

    if (optionalSpans.isEmpty()) {
      return this.addProhibitedSpans(conjunctionSpans);
    }
    else {
      return new ReqOptSpans(
        this.addProhibitedSpans(conjunctionSpans),
        optionalSpans.size() == 1 ? optionalSpans.get(0)
                                  : new OrSpans(optionalSpans),
        this);
    }
  }

  private Spans buildConjunctionSpans() throws IOException {
    if (requiredSpans.size() == 1) {
      return requiredSpans.get(0);
    }
    else {
      if (inOrder) {
        return new NearSpansOrdered(requiredSpans, slop);
      }
      else {
        return new NearSpansUnordered(requiredSpans, slop);
      }
    }
  }

  /**
   * Returns a spans that will use the provided required spans and the prohibited spans.
   *
   * @param requiredSpans
   *          A required spans already built.
   */
  private Spans addProhibitedSpans(final Spans requiredSpans)
  throws IOException {
    return (prohibitedSpans.size() == 0)
      ? requiredSpans // no prohibited
      : new NotSpans(requiredSpans,
                     (prohibitedSpans.size() == 1) ? prohibitedSpans.get(0)
                                                   : new OrSpans(prohibitedSpans));
  }

  @Override
  public int doc() {
    return parentSpans.doc();
  }

  @Override
  public IntsRef node() {
    return parentSpans.node();
  }

  @Override
  public boolean nextCandidateDocument() throws IOException {
    return parentSpans.nextCandidateDocument();
  }

  @Override
  public boolean nextNode() throws IOException {
    return parentSpans.nextNode();
  }

  @Override
  public boolean nextPosition() throws IOException {
    return parentSpans.nextPosition();
  }

  @Override
  public float scoreInNode()
  throws IOException {
    return parentSpans.scoreInNode();
  }

  @Override
  public int getSlop() {
    return parentSpans.getSlop();
  }

  @Override
  public boolean skipToCandidate(final int target) throws IOException {
    return parentSpans.skipToCandidate(target);
  }

  @Override
  public int start() {
    return parentSpans.start();
  }

  @Override
  public int end() {
    return parentSpans.end();
  }

  @Override
  public String toString() {
    return parentSpans.toString();
  }

}

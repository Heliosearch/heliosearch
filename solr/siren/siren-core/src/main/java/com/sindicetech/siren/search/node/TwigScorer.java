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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.sindicetech.siren.search.node.TwigQuery.TwigWeight;

/**
 * An extension of {@link NodeBooleanScorer} that matches a root (ancestor)
 * scorer with a boolean combination of descendant scorers.
 *
 * <p>
 *
 * The {@link TwigScorer} subclasses the {@link NodeBooleanScorer}. A Twig query
 * is rewritten into a pure boolean query. To achieve this, we perform the
 * following:
 * <ul>
 * <li> The descendant scorers are filtered so that they return potential common
 * ancestors. Such a filtering is performed by {@link AncestorFilterScorer}.
 * <li> The root scorer is added as a required clause into the boolean query.
 * </ul>
 */
class TwigScorer extends NodeBooleanScorer {

  /**
   * Creates a {@link TwigScorer} with the given root scorer and lists of
   * required, prohibited and optional scorers.
   *
   * @param weight
   *          The BooleanWeight to be used.
   * @param root
   *          The scorer of the twig root.
   * @param rootLevel
   *          The level of the twig root.
   * @param required
   *          the list of required scorers.
   * @param prohibited
   *          the list of prohibited scorers.
   * @param optional
   *          the list of optional scorers.
   */
  public TwigScorer(final TwigWeight weight,
                    final NodeScorer root, final int rootLevel,
                    final List<NodeScorer> required,
                    final List<NodeScorer> prohibited,
                    final List<NodeScorer> optional) throws IOException {
    super(weight,
      append(addAncestorFilter(required, rootLevel), root),
      addAncestorFilter(prohibited, rootLevel),
      addAncestorFilter(optional, rootLevel));
  }

  /**
   * Creates a {@link TwigScorer} with no root scorer and with lists of
   * required, prohibited and optional scorers.
   *
   * @param weight
   *          The BooleanWeight to be used.
   * @param rootLevel
   *          The level of the twig root.
   * @param required
   *          the list of required scorers.
   * @param prohibited
   *          the list of prohibited scorers.
   * @param optional
   *          the list of optional scorers.
   */
  public TwigScorer(final TwigWeight weight,
                    final int rootLevel,
                    final List<NodeScorer> required,
                    final List<NodeScorer> prohibited,
                    final List<NodeScorer> optional) throws IOException {
    super(weight,
      addAncestorFilter(required, rootLevel),
      addAncestorFilter(prohibited, rootLevel),
      addAncestorFilter(optional, rootLevel));
  }

  private static final List<NodeScorer> addAncestorFilter(final List<NodeScorer> scorers,
                                                          final int ancestorLevel) {
    final ArrayList<NodeScorer> filteredScorers = new ArrayList<NodeScorer>();
    for (final NodeScorer scorer : scorers) {
      filteredScorers.add(new AncestorFilterScorer(scorer, ancestorLevel));
    }
    return filteredScorers;
  }

  private static final List<NodeScorer> append(final List<NodeScorer> scorers,
                                               final NodeScorer scorer) {
    scorers.add(scorer);
    return scorers;
  }

  @Override
  public String toString() {
    return "TwigScorer(" + this.weight + "," + this.doc() + "," +
      this.node() + ")";
  }

}

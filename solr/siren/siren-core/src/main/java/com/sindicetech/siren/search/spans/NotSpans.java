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

import com.sindicetech.siren.util.NodeUtils;

import java.io.IOException;

/**
 * {@link com.sindicetech.siren.search.spans.Spans} for {@link NotSpanQuery} excludes matching positions that are
 * overlapping with positions of the excluded spans.
 */
class NotSpans extends Spans {

  private final Spans includeSpans;
  private final Spans excludeSpans;

  private boolean moreExclude = false;

  private final int pre;
  private final int post;

  public NotSpans(final Spans includeSpans, final Spans excludeSpans) throws IOException {
    this(includeSpans, excludeSpans, 0, 0);
  }

  public NotSpans(final Spans includeSpans, final Spans excludeSpans, final int pre, final int post)
  throws IOException {
    this.includeSpans = includeSpans;
    this.excludeSpans = excludeSpans;
    this.pre = pre;
    this.post = post;
  }

  @Override
  public boolean nextCandidateDocument() throws IOException {
    if (!this.includeSpans.nextCandidateDocument()) {
      return false;
    }

    // advance the excluded span to or after the included candidate document
    if (includeSpans.doc() > excludeSpans.doc()) {
      moreExclude = excludeSpans.skipToCandidate(includeSpans.doc());
    }

    return true;
  }

  @Override
  public boolean nextNode() throws IOException {
    if (!includeSpans.nextNode()) { // Move to the next matching node
      return false; // exhausted, nothing left
    }

    if (includeSpans.doc() != excludeSpans.doc()) {
      return true; // previous call to includeSpans.nextNode() has returned true
    }

    // advance the excluded span to or after the included candidate node
    while (NodeUtils.compare(includeSpans.node(), excludeSpans.node()) > 0) {
      if (!(moreExclude = excludeSpans.nextNode())) {
        return true;
      }
    }

    return true;
  }

  @Override
  public boolean nextPosition() throws IOException {
    // move the included spans to its next position
    if (!includeSpans.nextPosition()) {
      // no match for included spans
      return false;
    }

    // try to find a non excluded position
    return this.toNonExcludedPosition();
  }

  /**
   * Advances includeSpans to the next non excluded position, if any. {@link #includeSpans} must have been advanced
   * once to a new position using {@link #nextPosition()}.
   *
   * @return true iff the current candidate document and node has a non excluded position.
   */
  private boolean toNonExcludedPosition() throws IOException {
    boolean moreInclude = true;

    while (moreInclude) {
      // moving exclude position to find if there is an overlap
      while (moreExclude && (excludeSpans.start() == -1 || excludeSpans.end() <= includeSpans.start() - pre)) {
        moreExclude = excludeSpans.nextPosition();
      }
      // excluded
      if (moreExclude && excludeSpans.start() < includeSpans.end() + post) {
        // try another include position
        moreInclude = includeSpans.nextPosition();
      }
      else {
        return moreInclude;
      }
    }

    return moreInclude;
  }

  @Override
  public boolean skipToCandidate(int target) throws IOException {
    if (!(includeSpans.skipToCandidate(target))) {
      return false;
    }

    if (includeSpans.doc() > excludeSpans.doc()) {
      moreExclude = excludeSpans.skipToCandidate(includeSpans.doc());
    }

    return true;
  }

  @Override
  public float scoreInNode() throws IOException {
    return includeSpans.scoreInNode();
  }

  @Override
  public int getSlop() {
    return includeSpans.getSlop();
  }

  @Override
  public int doc() {
    return includeSpans.doc();
  }

  @Override
  public IntsRef node() {
    return includeSpans.node();
  }

  @Override
  public int start() {
    return includeSpans.start();
  }

  @Override
  public int end() {
    return includeSpans.end();
  }

}

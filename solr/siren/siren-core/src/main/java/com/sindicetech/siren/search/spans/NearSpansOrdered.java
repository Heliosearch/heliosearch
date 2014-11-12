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

import com.sindicetech.siren.index.PositionsIterator;

import java.io.IOException;
import java.util.List;

/**
 * A {@link com.sindicetech.siren.search.spans.Spans} that is formed from the ordered subspans of a
 * {@link com.sindicetech.siren.search.spans.NearSpanQuery}
 * where the subspans do not overlap and have a maximum slop between them.
 * <p>
 * The formed spans only contains minimum slop matches.<br>
 * The matching slop is computed from the distance(s) between
 * the non overlapping matching {@link com.sindicetech.siren.search.spans.Spans}.<br>
 * Successive matches are always formed from the successive {@link com.sindicetech.siren.search.spans.Spans}
 * of the {@link com.sindicetech.siren.search.spans.NearSpanQuery}.
 * <p>
 * The formed spans may contain overlaps when the slop is at least 1.
 * For example, when querying using
 * <pre>t1 t2 t3</pre>
 * with slop at least 1, the fragment:
 * <pre>t1 t2 t1 t3 t2 t3</pre>
 * matches twice:
 * <pre>t1 t2 .. t3      </pre>
 * <pre>      t1 .. t2 t3</pre>
 */
class NearSpansOrdered extends NearSpans {

  private int matchSlop;

  public NearSpansOrdered(final List<Spans> spans, final int slop) throws IOException {
    super(spans, slop);
  }

  @Override
  public boolean nextPosition() throws IOException {
    // Initialise the first spans
    if (!subSpans[0].nextPosition()) {
      matchStart = matchEnd = PositionsIterator.NO_MORE_POS;
      return false;
    }

    // Ensure that all the spans are ordered
    while (stretchToOrder()) {
      // check if the spans are within the allowed slop
      if (shrinkToAfterShortestMatch()) {
        return true;
      }
      // the spans were not in the allowed slop, try the next one
      // subSpans[0] has been advanced after the possible match, no need to call subSpans[0].nextPosition()
    }

    // if we are out of the loop, then this means that one of the sub-span is exhausted
    matchStart = matchEnd = PositionsIterator.NO_MORE_POS;
    return false;
  }

  /**
   * Order the subSpans within the same document and same node by advancing all later spans
   * after the previous one.
   */
  private boolean stretchToOrder() throws IOException {
    for (int i = 1; i < subSpans.length; i++) {
      while (!isSpansOrdered(subSpans[i-1].start(), subSpans[i-1].end(), subSpans[i].start(), subSpans[i].end())) {
        if (!subSpans[i].nextPosition()) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * The subSpans are ordered in the same doc, so there is a possible match.
   * Compute the slop while making the match as short as possible by advancing
   * all subSpans except the last one in reverse order.
   */
  private boolean shrinkToAfterShortestMatch() throws IOException {
    matchStart = subSpans[subSpans.length - 1].start();
    matchEnd = subSpans[subSpans.length - 1].end();

    this.matchSlop = 0;
    int lastStart = matchStart;
    int lastEnd = matchEnd;

    for (int i = subSpans.length - 2; i >= 0; i--) {
      Spans prevSpans = subSpans[i];
      int prevStart = prevSpans.start();
      int prevEnd = prevSpans.end();

      // Advance prevSpans until after (lastStart, lastEnd) to check if the latest (prevStart, prevEnd)
      // is the closest one to (lastStart, lastEnd).
      while (true) {
        if (!prevSpans.nextPosition()) {
          // prevSpans is exhausted. We need to check the remaining subSpans with the current (prevStart, prevEnd) for
          // final match.
          break;
        }
        else {
          int ppStart = prevSpans.start();
          int ppEnd = prevSpans.end();
          if (!isSpansOrdered(ppStart, ppEnd, lastStart, lastEnd)) {
            // prevSpans is after (lastStart, lastEnd). We need to check the remaining subSpans with the current
            // (prevStart, prevEnd) for final match.
            break;
          }
          // prevSpans still before (lastStart, lastEnd), Update (prevStart, prevEnd) and try the next position.
          else {
            prevStart = ppStart;
            prevEnd = ppEnd;
          }
        }
      }

      assert prevStart <= matchStart;
      if (matchStart > prevEnd) { // Only non overlapping spans add to slop.
        matchSlop += (matchStart - prevEnd);
      }

      // Do not break on (matchSlop > allowedSlop) here to make sure
      // that subSpans[0] is advanced after the match, if any.
      matchStart = prevStart;
      lastStart = prevStart;
      lastEnd = prevEnd;
    }

    boolean match = this.matchSlop <= this.allowedSlop;

    return match; // ordered and allowed slop
  }

  @Override
  protected int getSlop() {
    return matchSlop;
  }

}


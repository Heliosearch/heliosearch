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

import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;

/**
 * Checks to see if the {@link PositionCheckSpanQuery#getMatch()} lies between a start and end position
 */
public class PositionRangeSpanQuery extends PositionCheckSpanQuery {

  protected int start = 0;
  protected int end;

  public PositionRangeSpanQuery(SpanQuery match, int start, int end) {
    super(match);
    this.start = start;
    this.end = end;
  }

  @Override
  protected AcceptStatus acceptPosition(Spans spans) throws IOException {
    assert spans.start() != spans.end();
    if (spans.start() >= start && spans.end() <= end) {
      return AcceptStatus.YES;
    } else {
      return AcceptStatus.NO;
    }
  }

  /**
   * @return The minimum position permitted in a match
   */
  public int getStart() {
    return start;
  }

  /**
   * @return The maximum end position permitted in a match.
   */
  public int getEnd() {
    return end;
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("spanPosRange(");
    buffer.append(match.toString(field));
    buffer.append(", ").append(start).append(", ");
    buffer.append(end);
    buffer.append(")");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public PositionRangeSpanQuery clone() {
    final PositionRangeSpanQuery clone = (PositionRangeSpanQuery) super.clone();
    clone.match = (SpanQuery) this.match.clone();
    return clone;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PositionRangeSpanQuery)) return false;
    final PositionRangeSpanQuery other = (PositionRangeSpanQuery) o;
    return (this.getBoost() == other.getBoost()) &&
            this.match.equals(other.match) &&
            this.end == other.end &&
            this.start == other.start &&
            this.levelConstraint == other.levelConstraint &&
            this.lowerBound == other.lowerBound &&
            this.upperBound == other.upperBound;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + Float.floatToIntBits(this.getBoost());
    result = prime * result + match.hashCode();
    result = prime * result + lowerBound;
    result = prime * result + upperBound;
    result = prime * result + levelConstraint;
    result = prime * result + start;
    result = prime * result + end;
    return result;
  }

}

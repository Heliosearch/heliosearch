/**
 * Query API to search term and node spans.
 *
 * <h2>Introduction</h2>
 *
 * This package contains the API for building queries to search spans or intervals of
 * nodes.
 *
 * <h2>Span Basics</h2>
 *
 * <p>
 *
 *   A span is a <code>&lt;doc,node,startPosition,endPosition&gt;</code> tuple. A span is represented by the
 *   {@link com.sindicetech.siren.search.spans.Spans} class. There are two types of span:
 *   <ul>
 *     <li><b>{@link com.sindicetech.siren.search.spans.TermSpans}</b> which represents a span over the positions of one or
 *     more terms. All the terms of a term span must belong to the same node. The node element of a term span tuple is
 *     therefore the node containing the terms.</li>
 *     <li><b>{@link com.sindicetech.siren.search.spans.NodeSpans}</b> which represents a span over the positions of one or
 *     more child nodes. All the child nodes of a node span must have the same parent node. The node element of a node
 *     span tuple is therefore the parent node of the child nodes.</li>
 *   </ul>
 *
 *  In all cases, output spans are minimally inclusive.  In other words, a
 *  span formed by matching a span in x and y starts at the lesser of the
 *  two starts and ends at the greater of the two ends.
 *
 * </p>
 *
 * <h2>Query Classes</h2>
 *
 * <p>Span query operators are represented by the {@link com.sindicetech.siren.search.spans.SpanQuery} class. This class
 * is a subclass of {@link com.sindicetech.siren.search.node.NodeQuery}, which means that you can combine
 * {@link com.sindicetech.siren.search.spans.SpanQuery} with {@link com.sindicetech.siren.search.node.NodeQuery}. However, the
 * inverse is not possible, apart with the {@link com.sindicetech.siren.search.spans.NodeSpanQuery}. The following span
 * query operators are implemented:
 *
 * <h3>{@link com.sindicetech.siren.search.spans.TermSpanQuery}</h3>
 *
 * A {@link com.sindicetech.siren.search.spans.TermSpanQuery} matches all spans
 * containing a particular {@link org.apache.lucene.index.Term}.
 *
 * <h3>{@link com.sindicetech.siren.search.spans.NodeSpanQuery}</h3>
 *
 * A {@link com.sindicetech.siren.search.spans.NodeSpanQuery} matches all spans
 * containing nodes matching a particular {@link com.sindicetech.siren.search.node.NodeQuery}.
 *
 * <h3>{@link com.sindicetech.siren.search.spans.NearSpanQuery}</h3>
 *
 * A {@link com.sindicetech.siren.search.spans.NearSpanQuery} matches spans
 * which occur near one another, and can be used to implement things like
 * phrase search (when constructed from {@link com.sindicetech.siren.search.spans.TermSpanQuery}s),
 * node proximity (when constructed from {@link com.sindicetech.siren.search.spans.NodeSpanQuery}s).
 * {@link com.sindicetech.siren.search.spans.NearSpanQuery} supports nesting and can be used to implement more
 * complex search such as inter-phrase proximity.
 *
 * <h3>{@link com.sindicetech.siren.search.spans.OrSpanQuery}</h3>
 *
 * A {@link org.apache.lucene.search.spans.SpanOrQuery SpanOrQuery} merges spans from a
 * number of other {@link com.sindicetech.siren.search.spans.SpanQuery}s.
 *
 * <h3>{@link com.sindicetech.siren.search.spans.NotSpanQuery}</h3>
 *
 * A {@link com.sindicetech.siren.search.spans.NotSpanQuery} removes spans
 * matching one {@link com.sindicetech.siren.search.spans.SpanQuery} which overlap (or comes
 * near) another.
 *
 * <h3>{@link com.sindicetech.siren.search.spans.PositionRangeSpanQuery}</h3>
 *
 * A {@link com.sindicetech.siren.search.spans.PositionRangeSpanQuery} matches spans
 * matching a {@link com.sindicetech.siren.search.spans.SpanQuery} whose start position is superior to <code>start</code>
 * and end position is less than <code>end</code>. This can be used to constrain matches to arbitrary portions of the
 * document.
 *
 * <h3>{@link com.sindicetech.siren.search.spans.BooleanSpanQuery}</h3>
 *
 * A {@link com.sindicetech.siren.search.spans.BooleanSpanQuery} matches a boolean combination of spans with proximity
 * and order cosntraints.
 *
 * <h2>Examples</h2>
 *
 * <p>For example, a span query which matches "John Kerry" within ten
 * words of "George Bush" within the first 100 words of the document
 * could be constructed with:
 * <pre class="prettyprint">
 * SpanQuery john   = new TermSpanQuery(new Term("content", "john"));
 * SpanQuery kerry  = new TermSpanQuery(new Term("content", "kerry"));
 * SpanQuery george = new TermSpanQuery(new Term("content", "george"));
 * SpanQuery bush   = new TermSpanQuery(new Term("content", "bush"));
 *
 * BooleanSpanQuery johnKerry = new BooleanSpanQuery(0, true);
 * johnKerry.add(john, NodeBooleanClause.Occur.MUST);
 * johnKerry.add(johnKerry, NodeBooleanClause.Occur.MUST);
 *
 * BooleanSpanQuery georgeBush = new BooleanSpanQuery(0, true);
 * johnKerry.add(george, NodeBooleanClause.Occur.MUST);
 * johnKerry.add(bush, NodeBooleanClause.Occur.MUST);
 *
 * BooleanSpanQuery johnKerryNearGeorgeBush = new BooleanSpanQuery(10, false);
 * johnKerryNearGeorgeBush.add(johnKerry, NodeBooleanClause.Occur.MUST);
 * johnKerryNearGeorgeBush.add(georgeBush, NodeBooleanClause.Occur.MUST);
 *
 * SpanQuery johnKerryNearGeorgeBushAtStart =
 * new PositionRangeSpanQuery(johnKerryNearGeorgeBush, 100, Integer.MAX_VALUE);
 * </pre>
 *
 * <p>Span queries may be freely intermixed with other SIREn queries.
 * So, for example, the above query can be restricted to nodes which
 * also use the word "iraq" with:
 *
 * <pre class="prettyprint">
 * NodeQuery query = new NodeBooleanQuery();
 * query.add(johnKerryNearGeorgeBushAtStart, true, false);
 * query.add(new NodeTermQuery("content", "iraq"), true, false);
 * </pre>
 */
package com.sindicetech.siren.search.spans;


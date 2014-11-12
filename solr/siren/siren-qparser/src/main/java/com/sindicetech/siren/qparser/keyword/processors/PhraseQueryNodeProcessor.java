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
package com.sindicetech.siren.qparser.keyword.processors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.*;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;
import org.apache.lucene.util.Version;

import com.sindicetech.siren.qparser.keyword.nodes.DatatypeQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.ProtectedQueryNode;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

/**
 * This processor analyses a {@link FieldQueryNode} using a {@link WhitespaceAnalyzer}
 * and outputs a {@link TokenizedPhraseQueryNode} if more than one token are returned.
 *
 * <p>
 *
 * This processor operates on every {@link FieldQueryNode} that is not
 * {@link WildcardQueryNode}, {@link FuzzyQueryNode} or
 * {@link RangeQueryNode} contained in the query node tree, and it applies
 * a {@link WhitespaceAnalyzer} to that {@link FieldQueryNode} object.
 *
 * <p>
 *
 * If the analyzer returns only one term, the node is returned unchanged.
 *
 * <p>
 *
 * If the analyzer returns more than one term, a {@link TokenizedPhraseQueryNode}
 * is created and returned.
 *
 * <p>
 *
 * If no term is returned by the analyzer, a {@link NoTokenFoundQueryNode} object
 * is returned.
 */
public class PhraseQueryNodeProcessor extends QueryNodeProcessorImpl {

  private final Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_46);

  @Override
  protected QueryNode postProcessNode(final QueryNode node) throws QueryNodeException {
    if (node instanceof TextableQueryNode
    && !(node instanceof WildcardQueryNode)
    && !(node instanceof FuzzyQueryNode)
    && !(node instanceof RegexpQueryNode)
    && !(node instanceof ProtectedQueryNode)
    && !(node.getParent() instanceof RangeQueryNode)) {

      final FieldQueryNode fieldNode = ((FieldQueryNode) node);
      final String text = fieldNode.getTextAsString();
      final String field = fieldNode.getFieldAsString();

      final TokenStream source;
      try {
        source = this.analyzer.tokenStream(field, new StringReader(text));
        source.reset();
      }
      catch (final IOException e1) {
        throw new RuntimeException(e1);
      }
      final CachingTokenFilter buffer = new CachingTokenFilter(source);

      int numTokens = 0;
      try {
        while (buffer.incrementToken()) {
          numTokens++;
        }
      } catch (final IOException e) {
        // ignore
      }

      try {
        // rewind the buffer stream
        buffer.reset();
        // close original stream - all tokens buffered
        source.close();
      } catch (final IOException e) {
        // ignore
      }

      if (!buffer.hasAttribute(CharTermAttribute.class)) {
        return new NoTokenFoundQueryNode();
      }
      final CharTermAttribute termAtt = buffer.getAttribute(CharTermAttribute.class);

      if (numTokens == 0) {
        return new NoTokenFoundQueryNode();
      }
      // phrase query
      else if (numTokens != 1) {
        String datatype = (String) DatatypeProcessor.getDatatype(this.getQueryConfigHandler(), node);
        final TokenizedPhraseQueryNode pq = new TokenizedPhraseQueryNode();
        // assign datatype
        pq.setTag(DatatypeQueryNode.DATATYPE_TAGID, datatype);

        for (int i = 0; i < numTokens; i++) {
          String term = null;

          try {
            final boolean hasNext = buffer.incrementToken();
            assert hasNext == true;
            term = termAtt.toString();

          } catch (final IOException e) {
            // safe to ignore, because we know the number of tokens
          }

          final FieldQueryNode newFieldNode = new FieldQueryNode(field, term, -1, -1);
          // set position increment
          newFieldNode.setPositionIncrement(i);
          // assign datatype
          newFieldNode.setTag(DatatypeQueryNode.DATATYPE_TAGID, datatype);
          pq.add(newFieldNode);
        }
        return pq;
      }
    }
    return node;
  }

  @Override
  protected QueryNode preProcessNode(final QueryNode node) throws QueryNodeException {
    return node;
  }

  @Override
  protected List<QueryNode> setChildrenOrder(final List<QueryNode> children)
      throws QueryNodeException {
    return children;
  }

}

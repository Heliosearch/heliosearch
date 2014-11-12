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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.*;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;
import org.apache.lucene.search.MultiPhraseQuery;

import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys;
import com.sindicetech.siren.qparser.keyword.nodes.DatatypeQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.TwigQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.WildcardNodeQueryNode;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

/**
 * This processor analyzes query terms based on their datatype.
 *
 * <p>
 *
 * This processor retrieves the {@link Analyzer} associated with the TAG
 * {@link DatatypeQueryNode#DATATYPE_TAGID} in the key
 * {@link KeywordConfigurationKeys#DATATYPES_ANALYZERS}, and uses it on the
 * {@link FieldQueryNode} text, which is not a {@link WildcardQueryNode},
 * a {@link FuzzyQueryNode}, a {@link RegexpQueryNode} or the bound of a
 * {@link RangeQueryNode}.
 *
 * <p>
 *
 * If no term is returned by the analyzer, a {@link NoTokenFoundQueryNode} object
 * is returned. An {@link WildcardNodeQueryNode} is returned instead if an ancestor
 * is a {@link TwigQueryNode}.
 *
 * <p>
 *
 * If the analyzer returns only one term, the
 * returned term is set to the {@link FieldQueryNode} and it is returned.
 *
 * <p>
 *
 * If the analyzer returns more than one term at different positions, a
 * {@link TokenizedPhraseQueryNode} is created. If they are all at the same
 * position, a {@link OrQueryNode} object is created and returned.
 *
 * <p>
 *
 * If the analyzer returns multiple terms and the parent node is a
 * {@link TokenizedPhraseQueryNode}, a {@link QueryNodeException} is thrown
 * because {@link MultiPhraseQuery} are not supported in SIREn.
 */
public class DatatypeAnalyzerProcessor extends QueryNodeProcessorImpl {

  private int nbTwigs = 0;

  @Override
  protected QueryNode preProcessNode(final QueryNode node)
  throws QueryNodeException {
    if (node instanceof TwigQueryNode) {
      nbTwigs++;
    }
    return node;
  }

  @Override
  protected QueryNode postProcessNode(final QueryNode node)
  throws QueryNodeException {
    if (node instanceof TextableQueryNode &&
      !(node instanceof WildcardQueryNode) &&
      !(node instanceof FuzzyQueryNode) &&
      !(node instanceof RegexpQueryNode) &&
      !(node.getParent() instanceof RangeQueryNode)) {

      final FieldQueryNode fieldNode = ((FieldQueryNode) node);
      final String field = fieldNode.getFieldAsString();
      final String datatype = DatatypeProcessor.getDatatype(this.getQueryConfigHandler(), node);
      if (datatype == null) {
        return node;
      }
      final Analyzer analyzer = this.getAnalyzer(datatype);

      TokenBuffer buffer = new TokenBuffer(analyzer, fieldNode);

      if (!buffer.hasCharTermAttribute()) {
        return new NoTokenFoundQueryNode();
      }

      switch (buffer.getNumTokens()) {
        case 0:
          return this.toEmptyQueryNode();

        case 1:
          fieldNode.setText(buffer.getFirstTerm());
          return fieldNode;

        default:
          final LinkedList<QueryNode> children = buffer.getFieldQueryNodes(field, datatype,
            this.isPositionIncrementsEnabled());

          // Check for phrase query
          if (node.getParent() instanceof TokenizedPhraseQueryNode) {
            throw new QueryNodeException(new MessageImpl("Cannot build a MultiPhraseQuery"));
          }

          // If multiple terms at one single position, this must be a query
          // expansion. Perform a OR between the terms.
          if (buffer.hasSeveralTokensAtSamePosition() && buffer.getPositionCount() == 1) {
            OrQueryNode or = new OrQueryNode(children);
            GroupQueryNode group = new GroupQueryNode(or);
            // assign datatype
            or.setTag(DatatypeQueryNode.DATATYPE_TAGID, datatype);
            group.setTag(DatatypeQueryNode.DATATYPE_TAGID, datatype);
            return group;
          }
          // if several tokens at same position && position count > 1, then
          // results can be unexpected
          else {
            final TokenizedPhraseQueryNode pq = new TokenizedPhraseQueryNode();
            for (int i = 0; i < children.size(); i++) {
              pq.add(children.get(i));
            }
            // assign datatype
            pq.setTag(DatatypeQueryNode.DATATYPE_TAGID, datatype);
            return pq;
          }
      }
    }
    else if (node instanceof TwigQueryNode) {
      nbTwigs--;
      assert nbTwigs >= 0;
    }
    return node;
  }

  private boolean isPositionIncrementsEnabled() {
    QueryConfigHandler conf = this.getQueryConfigHandler();
    final Boolean positionIncrementsEnabled = this.getQueryConfigHandler().get(ConfigurationKeys.ENABLE_POSITION_INCREMENTS);
    if (positionIncrementsEnabled != null) {
      return positionIncrementsEnabled;
    }
    return false;
  }

  private Analyzer getAnalyzer(String datatype) throws QueryNodeException {
    final Analyzer analyzer = this.getQueryConfigHandler()
                                  .get(KeywordConfigurationKeys.DATATYPES_ANALYZERS)
                                  .get(datatype);
    if (analyzer == null) {
      throw new QueryNodeException(new MessageImpl(
        QueryParserMessages.INVALID_SYNTAX, "No analyzer associated with " + datatype));
    }

    return analyzer;
  }

  private CachingTokenFilter getBuffer(Analyzer analyzer, FieldQueryNode fieldNode) {
    final TokenStream source;
    final String text = fieldNode.getTextAsString();
    final String field = fieldNode.getFieldAsString();

    try {
      source = analyzer.tokenStream(field, new StringReader(text));
      source.reset();
    }
    catch (final IOException e1) {
      throw new RuntimeException(e1);
    }
    return new CachingTokenFilter(source);
  }

  private QueryNode toEmptyQueryNode() {
    if (nbTwigs != 0) { // Twig special case
      return new WildcardNodeQueryNode();
    }
    return new NoTokenFoundQueryNode();
  }

  @Override
  protected List<QueryNode> setChildrenOrder(final List<QueryNode> children)
  throws QueryNodeException {
    return children;
  }

  /**
   * Analyze the {@link FieldQueryNode}, caches the stream of tokens and computes some statistics about the tokens.
   */
  private static class TokenBuffer {

    final TokenStream source;
    final CachingTokenFilter buffer;

    CharTermAttribute termAtt = null;
    PositionIncrementAttribute posIncrAtt = null;
    int numTokens = 0;
    int positionCount = 0;
    boolean severalTokensAtSamePosition = false;

    public TokenBuffer(Analyzer analyzer, FieldQueryNode node) {
      try {
        source = analyzer.tokenStream(node.getFieldAsString(), new StringReader(node.getTextAsString()));
        source.reset();
      }
      catch (final IOException e1) {
        throw new RuntimeException(e1);
      }
      buffer = new CachingTokenFilter(source);

      if (buffer.hasAttribute(CharTermAttribute.class)) {
        termAtt = buffer.getAttribute(CharTermAttribute.class);
      }

      if (buffer.hasAttribute(PositionIncrementAttribute.class)) {
        posIncrAtt = buffer.getAttribute(PositionIncrementAttribute.class);
      }

      try {
        while (buffer.incrementToken()) {
          numTokens++;
          final int positionIncrement = (posIncrAtt != null) ? posIncrAtt.getPositionIncrement() : 1;
          if (positionIncrement != 0) {
            positionCount += positionIncrement;
          }
          else {
            severalTokensAtSamePosition = true;
          }
        }
      }
      catch (final IOException e) {
        // ignore
      }

      try {
        // rewind the buffer stream
        buffer.reset();
        // close original stream - all tokens buffered
        source.close();
      }
      catch (final IOException e) {
        // ignore
      }
    }

    public boolean hasCharTermAttribute() {
      return buffer.hasAttribute(CharTermAttribute.class);
    }

    public int getNumTokens() {
      return this.numTokens;
    }

    public int getPositionCount() {
      return this.positionCount;
    }

    public boolean hasSeveralTokensAtSamePosition() {
      return severalTokensAtSamePosition;
    }

    public String getFirstTerm() throws QueryNodeException {
      try {
        buffer.incrementToken();
        return termAtt.toString();
      }
      catch (final IOException e) {
        // should not occur
        throw new QueryNodeException(e);
      }
    }

    public String getNextTerm() throws QueryNodeException {
      try {
        final boolean hasNext = buffer.incrementToken();
        assert hasNext == true;
        return termAtt.toString();
      }
      catch (final IOException e) {
        // should not occur
        throw new QueryNodeException(e);
      }
    }

    public LinkedList<QueryNode> getFieldQueryNodes(String field, String datatype, boolean positionIncrementsEnabled)
    throws QueryNodeException {
      final LinkedList<QueryNode> children = new LinkedList<QueryNode>();
      int position = -1;

      // build children
      for (int i = 0; i < this.getNumTokens(); i++) {
        String term = this.getNextTerm();
        final int positionIncrement = 1;

        final FieldQueryNode newFieldNode = new FieldQueryNode(field, term, -1, -1);
        // assign datatype
        newFieldNode.setTag(DatatypeQueryNode.DATATYPE_TAGID, datatype);

        // set position increment
        if (positionIncrementsEnabled) {
          position += positionIncrement;
          newFieldNode.setPositionIncrement(position);
        }
        else {
          newFieldNode.setPositionIncrement(i);
        }

        children.add(newFieldNode);
      }

      return children;
    }

  }

}

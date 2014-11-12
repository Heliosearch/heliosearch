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

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.TextableQueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;

import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys;
import com.sindicetech.siren.qparser.keyword.nodes.DatatypeQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.ProtectedQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;

import java.util.List;
import java.util.Properties;

/**
 * This processor replaces QNames occurring in a {@link ProtectedQueryNode}, a {@link DatatypeQueryNode} and
 * a {@link RegexpQueryNode} by their namespace. QNames are allowed in {@link RegexpQueryNode} as there is no
 * problem of ambiguity (a regexp's term is enclosed by '/').
 *
 * <p>
 *
 * The QNames mapping is provided by {@link KeywordConfigurationKeys#QNAMES}.
 */
public class QNamesProcessor
extends QueryNodeProcessorImpl {

  /** The property file containing the mapping */
  private Properties          qnames;
  private final StringBuilder sb = new StringBuilder();

  @Override
  protected QueryNode preProcessNode(final QueryNode node)
  throws QueryNodeException {
    if (node instanceof ProtectedQueryNode || node instanceof DatatypeQueryNode || node instanceof RegexpQueryNode) {
      if (qnames == null) {
        qnames = this.getQueryConfigHandler().get(KeywordConfigurationKeys.QNAMES);
      }
      if (qnames == null) { // the KeywordConfigurationKeys.QNAMES_PATH is not set
        return node;
      }

      // Replace the qname
      final CharSequence text;
      if (node instanceof TextableQueryNode) {
        text = ((TextableQueryNode) node).getText();
      }
      else {
        text = ((DatatypeQueryNode) node).getDatatype();
      }

      if (replace(text)) {
        if (node instanceof TextableQueryNode) {
          final TextableQueryNode pqn = (TextableQueryNode) node;
          pqn.setText(sb.toString());
        }
        else {
          ((DatatypeQueryNode) node).setDatatype(sb.toString());
        }
      }
    }
    return node;
  }

  /**
   * Replace in the text the qualified name by its corresponding namespace.
   * This fills the attribute {@link #sb} with the text replacement.
   * @param text the String with the qualified name
   * @return true if there was a qualified name and that it has been replaced.
   */
  private boolean replace(CharSequence text) {
    final int termLength = text.length();
    int offset = 0;
    if ((offset = this.findDelimiter(text)) != termLength) {
      final CharSequence prefix = this.convertQName(text, offset);
      final CharSequence suffix = text.subSequence(offset + 1, termLength); // skip the QName delimiter
      sb.setLength(0);
      sb.append(prefix);
      sb.append(suffix);
      return true;
    }
    return false;
  }

  /**
   * Find the offset of the QName delimiter. If no delimiter is
   * found, return last offset, i.e., {@code termLength}.
   */
  protected int findDelimiter(final CharSequence c) {
    final int len = c.length();
    int ptr = 0;

    while (ptr < len - 1) {
      if (this.isQNameDelim(c.charAt(ptr))) {
        if (!this.isNameStartChar(c.charAt(ptr + 1))) {
          break; // if this is not a valid name start char, we can stop
        }
        return ptr;
      }
      ptr++;
    }

    return len;
  }

  /**
   * Based on <a>http://www.w3.org/TR/REC-xml/#NT-Name</a>
   */
  protected boolean isNameStartChar(final char c) {
    return c == ':' || c == '_' || Character.isLetter(c);
  }

  /**
   * Return <code>true</code> if the character is a colon.
   */
  protected boolean isQNameDelim(final char c) {
    return c == ':';
  }

  /**
   * Convert the QName to the associated namespace. If the prefix is not a
   * qname, it just returns the original prefix.
   */
  protected CharSequence convertQName(final CharSequence c, final int offset) {
    final String prefix = c.subSequence(0, offset).toString();

    if (qnames.containsKey(prefix)) {
      return qnames.getProperty(prefix);
    }
    return c.subSequence(0, offset + 1);
  }

  @Override
  protected QueryNode postProcessNode(final QueryNode node)
  throws QueryNodeException {
    return node;
  }

  @Override
  protected List<QueryNode> setChildrenOrder(final List<QueryNode> children)
  throws QueryNodeException {
    return children;
  }

}

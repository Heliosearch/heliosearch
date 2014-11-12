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
package com.sindicetech.siren.qparser.tree.builders.concise;

import com.sindicetech.siren.qparser.keyword.ConciseKeywordQueryParser;
import com.sindicetech.siren.qparser.keyword.ExtendedKeywordQueryParser;
import com.sindicetech.siren.qparser.tree.builders.ExtendedTreeQueryTreeBuilder;
import com.sindicetech.siren.qparser.tree.nodes.NodeQueryNode;
import com.sindicetech.siren.qparser.tree.nodes.TwigQueryNode;

/**
 * An extension of the {@link com.sindicetech.siren.qparser.tree.builders.ExtendedTreeQueryTreeBuilder} for the concise model.
 */
public class ConciseTreeQueryTreeBuilder extends ExtendedTreeQueryTreeBuilder {

  public ConciseTreeQueryTreeBuilder(final ConciseKeywordQueryParser keywordParser) {
    super(keywordParser);
  }

  @Override
  public void setBuilders(final ExtendedKeywordQueryParser keywordParser) {
    super.setBuilders(keywordParser);
    this.setBuilder(NodeQueryNode.class, new ConciseNodeQueryNodeBuilder((ConciseKeywordQueryParser) keywordParser));
    this.setBuilder(TwigQueryNode.class, new ConciseTwigQueryNodeBuilder((ConciseKeywordQueryParser) keywordParser));
  }

}

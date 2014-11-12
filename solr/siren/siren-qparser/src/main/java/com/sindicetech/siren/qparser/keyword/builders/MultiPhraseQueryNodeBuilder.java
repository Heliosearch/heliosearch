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
package com.sindicetech.siren.qparser.keyword.builders;

import org.apache.commons.lang.NotImplementedException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.MultiPhraseQueryNode;
import org.apache.lucene.search.MultiPhraseQuery;

/**
 * Builds a {@link MultiPhraseQuery} object from a {@link MultiPhraseQueryNode}
 * object.
 */
public class MultiPhraseQueryNodeBuilder implements KeywordQueryBuilder {

  public MultiPhraseQueryNodeBuilder() {
    // empty constructor
  }

  public MultiPhraseQuery build(QueryNode queryNode) throws QueryNodeException {
    throw new NotImplementedException("Multi phrase queries are not supported by SIRen yet");
//    
//    MultiPhraseQueryNode phraseNode = (MultiPhraseQueryNode) queryNode;
//
//    MultiPhraseQuery phraseQuery = new MultiPhraseQuery();
//
//    List<QueryNode> children = phraseNode.getChildren();
//
//    if (children != null) {
//      TreeMap<Integer, List<Term>> positionTermMap = new TreeMap<Integer, List<Term>>();
//
//      for (QueryNode child : children) {
//        FieldQueryNode termNode = (FieldQueryNode) child;
//        TermQuery termQuery = (TermQuery) termNode
//            .getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
//        List<Term> termList = positionTermMap.get(termNode
//            .getPositionIncrement());
//
//        if (termList == null) {
//          termList = new LinkedList<Term>();
//          positionTermMap.put(termNode.getPositionIncrement(), termList);
//
//        }
//
//        termList.add(termQuery.getTerm());
//
//      }
//
//      for (int positionIncrement : positionTermMap.keySet()) {
//        List<Term> termList = positionTermMap.get(positionIncrement);
//
//        phraseQuery.add(termList.toArray(new Term[termList.size()]),
//            positionIncrement);
//
//      }
//
//    }
//
//    return phraseQuery;

  }

}

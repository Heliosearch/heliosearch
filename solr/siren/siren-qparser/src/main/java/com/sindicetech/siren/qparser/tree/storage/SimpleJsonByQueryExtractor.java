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
package com.sindicetech.siren.qparser.tree.storage;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ContainerNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sindicetech.siren.search.node.*;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * <p>
 * simple implementation of {@link com.sindicetech.siren.qparser.tree.storage.JsonByQueryExtractor} that
 * uses only variable clause in a twig child
 * </p>
 * <p>
 * thread safe
 * </p>
 *
 *
 */
public class SimpleJsonByQueryExtractor implements JsonByQueryExtractor {

  private static final Logger logger = LoggerFactory.getLogger(SimpleJsonByQueryExtractor.class);
  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public String extractAsString(String json, Query query) throws ProjectionException {

    try {
      Tree<NodeQuery> queries = new Tree<NodeQuery>("root", null);
      List<String> variablesPaths = new ArrayList<String>();
      discoverRequiredJSONfieldsAndQueries(query, new LinkedList<String>(), variablesPaths, queries);
      if (variablesPaths.size() == 0) {
        return json;
      }
      JsonNode jsonRoot = mapper.readTree(json);
      if (queries.getChildren().size() == 0) {
        return mapper.writeValueAsString(projection(jsonRoot, variablesPaths));
      } else {
        return mapper.writeValueAsString(projection(buildFilteredTree(jsonRoot, queries),
            variablesPaths));
      }
    } catch (JsonGenerationException e) {
      throw new ProjectionException(e);
    } catch (JsonMappingException e) {
      throw new ProjectionException(e);
    } catch (IOException e) {
      throw new ProjectionException(e);
    }
  }

  private JsonNode buildFilteredTree(JsonNode inputJson, Tree<NodeQuery> tree)
      throws ProjectionException {

    if (inputJson.isArray()) {
      ArrayNode rootNode = mapper.createArrayNode();
      Iterator<JsonNode> elementsIterator = inputJson.getElements();

      while (elementsIterator.hasNext()) {
        JsonNode aNode = elementsIterator.next();
        JsonNode filteredNode = buildFilteredTree(aNode, tree);
        if (filteredNode != null) {
          rootNode.add(filteredNode);
        }
      }
      return rootNode;
    } else if (inputJson.isObject()) {
      ObjectNode rootNode = mapper.createObjectNode();
      Iterator<String> fieldNames = inputJson.getFieldNames();
      while (fieldNames.hasNext()) {
        String fieldName = fieldNames.next();
        if (tree.getChild(fieldName) == null) {
          rootNode.put(fieldName, inputJson.get(fieldName));
        } else {
          JsonNode filteredNode = buildFilteredTree(inputJson.get(fieldName),
              tree.getChild(fieldName));
          if (filteredNode == null) {
            return null;
          }
          rootNode.put(fieldName, filteredNode);
        }
      }
      return rootNode;
    } else {
      if (tree.getPayload() != null) {
        if (isMatching(inputJson, tree.getPayload(), null)) {
          return inputJson;
        } else {
          return null;
        }
      } else {
        throw new IllegalStateException("leaf of json and queries does not match");
      }
    }

  }

  private ContainerNode projection(JsonNode input, List<String> variablesPaths) {
    if (variablesPaths.size() == 1) {
      JsonNode projectedNode;
      if (variablesPaths.get(0).endsWith(".")) {
        projectedNode = projection(input,
            (variablesPaths.get(0).substring(0, variablesPaths.get(0).length() - 1)));
      } else {
        projectedNode = projection(input, (variablesPaths.get(0)));
      }
      if (projectedNode instanceof ArrayNode) {
        return (ContainerNode) projectedNode;
      } else {
        // wrap to object
        ObjectNode newRoot = mapper.createObjectNode();
        newRoot.put(lastPartOfPath(variablesPaths.get(0)), projectedNode);
        return newRoot;
      }
    }
    ObjectNode newRoot = mapper.createObjectNode();
    for (String path : variablesPaths) {
      if (path.endsWith(".")) {
        path = path.substring(0, path.length() - 1);
      }
      newRoot.put(lastPartOfPath(path), projection(input, path));
    }
    return newRoot;
  }

  private JsonNode projection(JsonNode node, String relativePath) {
    if (relativePath == null || relativePath.length() == 0) {
      return node;
    }
    if (node.isArray()) {
      ArrayNode arrayNode = mapper.createArrayNode();
      Iterator<JsonNode> elementsIterator = ((ArrayNode) node).getElements();

      while (elementsIterator.hasNext()) {
        ObjectNode oNode = mapper.createObjectNode();
        oNode.put(lastPartOfPath(relativePath), projection(elementsIterator.next(), relativePath));
        arrayNode.add(oNode);
      }
      return arrayNode;

    } else {
      int firstDotPosition = relativePath.indexOf('.');
      if (firstDotPosition == -1) {
        return node.get(relativePath);
      } else {
        return projection(node.get(relativePath.substring(0, firstDotPosition)),
            relativePath.substring(firstDotPosition + 1));
      }

    }

  }

  private String lastPartOfPath(String path) {
    int lastDotPosition = path.lastIndexOf('.');
    if (lastDotPosition == -1) {
      return path;
    } else {
      return path.substring(lastDotPosition + 1);
    }

  }

  private String getRoot(TwigQuery twigQuery) {
    if (twigQuery.getRoot() != null && !(twigQuery.getRoot() instanceof TwigQuery.EmptyRootQuery)) {
      Set<Term> terms = new HashSet<Term>();
      twigQuery.getRoot().extractTerms(terms);
      return terms.iterator().next().text();
    } else {
      return null;
    }
  }

  private boolean isMatching(JsonNode jsonNode, Query termOrRangeQuery, String fieldName)
      throws ProjectionException {
    if (termOrRangeQuery instanceof NodeTermQuery || termOrRangeQuery instanceof NodePrefixQuery) {
      Pattern termPat = null;
      if (termOrRangeQuery instanceof NodeTermQuery) {
        termPat = Pattern.compile(((NodeTermQuery) termOrRangeQuery).getTerm().text(),
            Pattern.CASE_INSENSITIVE);
      } else if (termOrRangeQuery instanceof NodePrefixQuery) {
        termPat = Pattern.compile(((NodePrefixQuery) termOrRangeQuery).getPrefix().text() + ".*",
            Pattern.CASE_INSENSITIVE);
      }
      if (jsonNode.isObject()) {
        if (termPat.matcher(jsonNode.get(fieldName).asText()).matches()) {
          return true;
        }
      } else {
        if (termPat.matcher(jsonNode.asText()).matches()) {
          return true;
        }
      }
      return false;
    } else if (termOrRangeQuery instanceof NodeNumericRangeQuery<?>) {
      return evaluateNumericRangeQuery(jsonNode, (NodeNumericRangeQuery<?>) termOrRangeQuery);
    }
    return false;

  }

  private <T extends Number> boolean evaluateNumericRangeQuery(JsonNode jsonNode,
      NodeNumericRangeQuery<T> termOrRangeQuery) throws ProjectionException {
    if (!jsonNode.isNumber()) {
      throw new ProjectionException("numering range used on not numeric field");
    }

    switch (jsonNode.getNumberType()) {
      case LONG :
        if (termOrRangeQuery.getMin() != null
            && termOrRangeQuery.getMin().longValue() > jsonNode.getNumberValue().longValue()) {
          return false;
        }
        if (termOrRangeQuery.getMax() != null
            && termOrRangeQuery.getMax().longValue() < jsonNode.getNumberValue().longValue()) {
          return false;
        }
        break;
      case INT :
        if (termOrRangeQuery.getMin() != null
            && termOrRangeQuery.getMin().intValue() > jsonNode.getNumberValue().intValue()) {
          return false;
        }
        if (termOrRangeQuery.getMax() != null
            && termOrRangeQuery.getMax().intValue() < jsonNode.getNumberValue().intValue()) {
          return false;
        }
        break;
      case FLOAT :
        if (termOrRangeQuery.getMin() != null
            && termOrRangeQuery.getMin().floatValue() > jsonNode.getNumberValue().floatValue()) {
          return false;
        }
        if (termOrRangeQuery.getMax() != null
            && termOrRangeQuery.getMax().floatValue() < jsonNode.getNumberValue().floatValue()) {
          return false;
        }
        break;
      case DOUBLE :
        if (termOrRangeQuery.getMin() != null
            && termOrRangeQuery.getMin().doubleValue() > jsonNode.getNumberValue().doubleValue()) {
          return false;
        }
        if (termOrRangeQuery.getMax() != null
            && termOrRangeQuery.getMax().doubleValue() < jsonNode.getNumberValue().doubleValue()) {
          return false;
        }
        break;
      default :
        // should never happen
        throw new IllegalArgumentException("Invalid numeric NumericType");
    }
    return true;
  }

  /**
   * It traverses through the query tree searching for "variable" and "query" clauses in a twig
   * child. When the variable close is find, its path is stored. When query is find, query with the
   * path is stored.
   *
   * @param inspectedQueryNode
   * @throws ProjectionException
   */
  private void discoverRequiredJSONfieldsAndQueries(Query inspectedQueryNode,
      LinkedList<String> actualJsonPath, List<String> variablesPaths, Tree<NodeQuery> queries)
      throws ProjectionException {

    if (inspectedQueryNode instanceof TwigQuery) {
      TwigQuery twigQuery = (TwigQuery) inspectedQueryNode;
      String fName = getRoot(twigQuery);
      if (fName != null) {
        actualJsonPath.add(fName);
      } else {
        actualJsonPath.add(".");
      }
      for (NodeBooleanClause clause : twigQuery.getClauses()) {
        discoverRequiredJSONfieldsAndQueries(clause.getQuery(), actualJsonPath, variablesPaths,
            queries);
      }

      actualJsonPath.removeLast();
      return;
    }

    if (inspectedQueryNode instanceof NodeBooleanQuery) {
      NodeBooleanQuery nodeBooleanQuery = (NodeBooleanQuery) inspectedQueryNode;
      for (NodeBooleanClause booleanClose : nodeBooleanQuery.clauses()) {
        discoverRequiredJSONfieldsAndQueries(booleanClose.getQuery(), actualJsonPath,
            variablesPaths, queries);
      }
      return;
    }

    if (inspectedQueryNode instanceof BooleanQuery) {
      BooleanQuery nodeBooleanQuery = (BooleanQuery) inspectedQueryNode;
      for (BooleanClause booleanClose : nodeBooleanQuery.clauses()) {
        discoverRequiredJSONfieldsAndQueries(booleanClose.getQuery(), actualJsonPath,
            variablesPaths, queries);
      }
      return;
    }

    if (inspectedQueryNode instanceof NodeVariableQuery) {
      if (actualJsonPath.size() > 0) {
        String actalPathAsString = concatanatePath(actualJsonPath);
        variablesPaths.add(actalPathAsString);
        logger.debug("variable found for field {}", actalPathAsString);
      } else {
        logger.warn("variable found but JSON field name is empty");
      }
      return;
    }

    // nothing interesting in following NodeQueries
    if (inspectedQueryNode instanceof NodeTermQuery
        || inspectedQueryNode instanceof NodePhraseQuery
        || inspectedQueryNode instanceof NodeNumericRangeQuery<?>
        || inspectedQueryNode instanceof NodePrefixQuery) {
      String actalPathAsString = concatanatePath(actualJsonPath);
      queries.insert(actualJsonPath, (NodeQuery) inspectedQueryNode);
      logger.debug("query found for field {}", actalPathAsString);
      return;
    }

    if (inspectedQueryNode instanceof LuceneProxyNodeQuery) {
      discoverRequiredJSONfieldsAndQueries(
          ((LuceneProxyNodeQuery) inspectedQueryNode).getNodeQuery(), actualJsonPath,
          variablesPaths, queries);
      return;
    }
    // unsupported query type
    logger.error("unsupported query type {}", inspectedQueryNode.getClass().getCanonicalName());
    throw new ProjectionException("unsupported query type"
        + inspectedQueryNode.getClass().getCanonicalName());
  }

  private String concatanatePath(List<String> parts) {
    StringBuilder sb = new StringBuilder();
    for (String part : parts.toArray(new String[0])) {
      sb.append(part);
    }
    return sb.toString();
  }
  /** intended for queries tree and for variables in future */
  private static class Tree<T> {
    T payload;
    String key;
    List<Tree<T>> children = new ArrayList<SimpleJsonByQueryExtractor.Tree<T>>();
    public T getPayload() {
      return payload;
    }
    public void setPayload(T payload) {
      this.payload = payload;
    }
    public String getKey() {
      return key;
    }
    public List<Tree<T>> getChildren() {
      return children;
    }
    public void addChild(Tree<T> child) {
      children.add(child);
    }
    public Tree(String key, T payload) {
      this.payload = payload;
      this.key = key;
    }

    public Tree<T> getChild(String key) {
      for (Tree<T> child : children) {
        if (child.getKey().equals(key)) {
          return child;
        }
      }
      return null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void insert(List<String> keys, T payload) {
      Tree<T> activeNode = this;
      for (String subpath : keys) {
        if (!subpath.equals(".")) {
          Tree<T> child = activeNode.getChild(subpath);
          if (child == null) {
            child = new Tree(subpath, null);
            activeNode.addChild(child);
          }
          activeNode = child;
        }
      }
      activeNode.setPayload(payload);
    }
  }
}

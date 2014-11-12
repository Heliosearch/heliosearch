/**
 * Create a {@link org.apache.lucene.search.Query} object
 * from the processed {@link org.apache.lucene.queryparser.flexible.core.nodes.QueryNode} tree
 * using a bottom-up approach.
 * 
 * Each {@link org.apache.lucene.queryparser.flexible.core.nodes.QueryNode} of the query tree
 * is mapped to a {@link com.sindicetech.siren.qparser.keyword.builders.KeywordQueryBuilder}.
 * Most builders create a {@link com.sindicetech.siren.search.node.NodeQuery} object.
 * In case the {@link com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys#ALLOW_TWIG}
 * is <code>true</code>, every {@link com.sindicetech.siren.search.node.NodeQuery} is wrapped
 * into a {@link com.sindicetech.siren.search.node.LuceneProxyNodeQuery}; otherwise, a pure
 * {@link com.sindicetech.siren.search.node.NodeQuery} object is created from the
 * query tree build.
 * 
 */
package com.sindicetech.siren.qparser.keyword.builders;


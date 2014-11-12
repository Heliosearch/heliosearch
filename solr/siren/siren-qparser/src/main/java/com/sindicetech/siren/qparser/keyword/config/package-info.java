/**
 * Set of parameters for configuring the behaviour of the query processing.
 * <p>
 * The processing of a query can be configured using a {@link org.apache.lucene.queryparser.flexible.core.config.ConfigurationKey}.
 * For example, the parameter {@link org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys#DEFAULT_OPERATOR}
 * sets the default {@link org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator} to use
 * when none is explicitly used in the query. In addition to the parameters of
 * the Lucene framework {@link org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys},
 * the keyword parser provides its own set of parameters in {@link com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys}.
 * </p>
 * 
 */
package com.sindicetech.siren.qparser.keyword.config;


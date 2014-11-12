/**
 * The SIREn query parser plugin
 *
 * <h2>Introduction</h2>
 *
 * This package contains the Solr plugin that integrates the SIREn search features with the Solr REST API. Both the
 * SIREN's JSON and Keyword query syntax are exposed through this API, each one managed respectively by
 * {@link com.sindicetech.siren.solr.qparser.tree.TreeQParserPlugin} and
 * {@link com.sindicetech.siren.solr.qparser.keyword.KeywordQParserPlugin}.
 *
 * <h2>Solr Configuration</h2>
 *
 * <p>
 * To activate these plugins in the Solr REST API, you have to modify your <code>solrconfg.xml</code> as follows:
 * </p>
 *
 * <h3>Extended Tree Query Parser</h3>
 *
 * <p>
 * First, you have to register the extended tree query parser plugin as follows:
 *
 * <pre style="overflow:auto;">{@code
 * <!-- Register SIREn's Extended Tree query parser. -->
 * <queryParser name="json" class="com.sindicetech.siren.solr.qparser.json.TreeQParserPlugin">
 *   <str name="qnames">qnames.txt</str>
 * </queryParser>
 * }</pre>
 *
 * Next, you can register a search handler to use the extended tree query parser to process a query:
 *
 * <pre style="overflow:auto;">{@code
 * <requestHandler name="json" class="solr.SearchHandler">
 *   <!-- default values for query parameters -->
 *   <lst name="defaults">
 *     <str name="defType">json</str>
 *   </lst>
 * </requestHandler>
 * }</pre>
 *
 * The extended tree query parser will be accessible through the resource <code>/json</code>.
 * </p>
 *
 * <h3>Concise Tree Query Parser</h3>
 *
 * <p>
 * If you are using the concise model, you must register the Concise tree query parser plugin as follows:
 *
 * <pre style="overflow:auto;">{@code
 * <!-- Register SIREn's Concise Tree query parser. -->
 * <queryParser name="json" class="com.sindicetech.siren.solr.qparser.json.TreeQParserPlugin">
 *   <str name="qnames">qnames.txt</str>
 * </queryParser>
 * }</pre>
 *
 * Next, register a search handler to use the concise tree query parser:
 *
 * <pre style="overflow:auto;">{@code
 * <requestHandler name="json" class="solr.SearchHandler">
 *   <!-- default values for query parameters -->
 *   <lst name="defaults">
 *     <str name="defType">json</str>
 *   </lst>
 * </requestHandler>
 * }</pre>
 *
 * The Concise Tree query parser will be accessible through the resource <code>/json</code>.
 * </p>
 *
 * <h3>Keyword Query Parser</h3>
 *
 * <p>
 * You can register the Keyword query parser plugin as follows:
 *
 * <pre style="overflow:auto;">{@code
 * <!-- Register SIREn's Keyword query parser. -->
 * <queryParser name="keyword" class="com.sindicetech.siren.solr.qparser.keyword.KeywordQParserPlugin">
 *    <str name="qnames">qnames.txt</str>
 * </queryParser>
 * }</pre>
 *
 * Next, register a search handler to use the concise tree query parser:
 *
 * <pre style="overflow:auto;">{@code
 * <requestHandler name="keyword" class="solr.SearchHandler">
 *   <!-- default values for query parameters -->
 *   <lst name="defaults">
 *     <str name="defType">keyword</str>
 *   </lst>
 * </requestHandler>
 * }</pre>
 *
 * The keyword query parser will be accessible through the resource <code>/keyword</code>.
 * </p>
 *
 * <h3>QNames Mapping</h3>
 *
 * <p>
 * A qnames mapping file can be provided using the parameter <code>qnames</code>. The parameter must indicate a file
 * that is located in the <code>conf</code> directory of your Solr home.
 * </p>
 *
 * <h2>Search Basics</h2>
 *
 * <p>
 * In addition to the <a href="https://cwiki.apache.org/confluence/display/solr/Common+Query+Parameters">Common Query
 * Parameters</a>, Faceting Parameters, Highlighting Parameters, and MoreLikeThis
 * Parameters, the SIREn's query parsers support the following parameters:
 *
 * <ul>
 * <li><b>q</b> Defines the raw input strings for the query.</li>
 * <li><b>q.op</b> Specifies the default operator for query expressions, overriding the default operator specified in
 * the schema.xml file. Possible values are "AND" or "OR".</li>
 * <li><b>df</b> Specifies a default field, overriding the definition of a default field in the schema.xml file.</li>
 * <li><b>qf</b> Specifies a list of fields in the index, each of which can be assigned a boost factor, on which to
 * perform the query. If absent, defaults to df.</li>
 * </ul>
 *
 * Obviously, the parameters <code>df</code> and <code>qf</code> must specify SIREn's fields.
 * The default parameter values can be specified in solrconfig.xml, and can be overridden by query-time values in the
 * request.
 * </p>
 *
 * <p>
 *
 * In order to execute a query through the API, you can use the keyword query parser as follows:
 *
 * <pre>
 *   /keyword?q=title : lucene&df=json
 * </pre>
 *
 * or the JSON query parser:
 *
 * <pre style="overflow:auto;">
 *   /json?q={"twig":{"root":"title","child":[{"occur":"MUST","node":{"query":"lucene"}}]}}&df=json
 * </pre>
 *
 * </p>
 *
 * <h2>Nested Queries</h2>
 *
 * <p>
 * The SIREn's parsers also support the Solr
 * <a href="https://cwiki.apache.org/confluence/display/solr/Other+Parsers#OtherParsers-NestedQueryParser">Nested Query
 * Parser</a> through the parameter <code>nested</code>. A Solr's nested query can be specify with the parameter
 * <code>nested</code> as shown below. This enables you to freely combine Solr's queries with SIREn's queries.
 *
 * <pre style="overflow:auto;">
 *   /keyword?q=title : lucene&df=json&nested={!lucene} timestamp:[NOW/DAY TO NOW]
 * </pre>
 *
 * The SIREn's parsers intersect the result of the main query specified by the parameter <code>q</code> with
 * the result of the nested query specified by the parameter <code>nested</code>.
 * </p>
 *
 * <p>
 * More than one <code>nested</code> parameter can be specified in the request. The result of each one them is
 * intersected with the result of the main query.
 * </p>
 *
 * <p>
 * It is not mandatory to specify a main query if you are specifying a nested query. This allows you for example
 * to execute the "match all" query using the lucene parser:
 * <pre>
 *   /keyword?nested={!lucene} *:*
 * </pre>
 * </p>
 */
package com.sindicetech.siren.solr.qparser;


/**
 * A keyword query parser implemented with the Lucene's Flexible Query Parser,
 * with support for proximity operators.
 *
 * <h2>Query Parser Syntax</h2>
 *
 * <p>
 * A keyword query is a boolean expression. The boolean expression can be a
 * mixture of primitive queries (e.g., a keyword
 * search) and of proximity queries.
 * </p>
 *
 * <p>
 * The syntax allows to use custom datatypes on any part of the query.
 * </p>
 *
 * <h3>Boolean</h3>
 *
 * <p>
 * A boolean expression follows the Lucene query syntax, except for the ':'
 * which does not define a field query but instead is used to build a twig
 * query.
 *
 * <pre>
 * a AND b
 * </pre>
 *
 * matches documents where the terms "a" and "b" occur in any node
 * of the JSON tree.
 * </p>
 *
 * <h3>Proximity</h3>
 *
 * <p>
 *   The query syntax extends the Lucene syntax for grouping to introduce proximity operators. It is possible
 *   to create boolean expressions that must satisfy some proximity constraints. These boolean expressions are similar to
 *   the phrase query in that they all restricts terms matches based on their position, but these boolean expressions
 *   are much more expressive. In order to activate proximity constraints on a boolean expression, you must append
 *   to the right parenthesis of the boolean group the character
 *   <code>~</code> or <code>#</code> with an integer. The integer indicates the maximum allowed slop, i.e., the
 *   maximum distance that is allowed among all the clauses of the boolean expressions. The character <code>#</code>
 *   indicates that the required clauses must match in order, while the character <code>~</code> indicates that they
 *   can match in any order.
 * </p>
 *
 * <p>
 *   For example, the expression:
 *   <pre>
 *     (+a +b)~0
 *   </pre>
 *   indicates that the two required terms <code>a</code> and <code>b</code> must
 *   match next to each other (slop of 0) in any order. On the other hand, the expression
 *   <pre>
 *     (a AND b)#1
 *   </pre>
 *   indicates that <code>a</code> and <code>b</code> must match in order with a maximum distance of 1 between them.
 * </p>
 *
 * <p>
 *   The syntax supports all the boolean operators, such as required (<code>+</code>, <code>AND</code>,
 *   <code>&&</code>), optional (<code>OR</code>, <code>||</code>) or excluded (<code>-</code>, <code>!</code>,
 *   <code>NOT</code>). However, when the order
 *   constraint is specified, it is only applicable
 *   to the required clauses, i.e., the optional and excluded clauses will match irrespective of their position. For
 *   example, with the expression
 *   <pre>
 *     (+a +b c)#0
 *   </pre>
 *   the optional term <code>c</code> can appear before or after the required terms <code>a</code> and
 *   <code>b</code>.
 * </p>
 *
 * <p>
 *   The slop constraint is applicable to the required terms, but also to the optional term if the boolean expression
 *   contains at least one required and one optional clause. For example, with the expression
 *   <pre>
 *     ( +a b )#1
 *   </pre>
 *   the optional term <code>b</code> will be considered in the scoring if and only if it is at a maximum distance of 1
 *   after the term <code>a</code>. For example, the text <code>a c b</code> will be scored higher than the text
 *   <code>a c d b</code>.
 * </p>
 *
 * <p>
 *   As with normal boolean group expressions, you can nest them arbitrarily:
 *   <pre>
 *     ( +a +(b c)#1 )~0
 *   </pre>
 *   You can nest boolean expressions with proximity constraints inside normal boolean expressions, but the inverse is
 *   invalid. For example, the expression
 *   <pre>
 *     ( a AND (b OR c)~0 )
 *   </pre>
 *   is valid, while the following expression is invalid.
 *   <pre>
 *     ( a AND (b OR c) )~0
 *   </pre>
 * </p>
 *
 * <p>
 *   These boolean expressions supports wildcard, fuzzy and range queries. For example, the expression containing
 *   wildcard and fuzzy term query is valid:
 *   <pre>
 *     ( +a /b*b/ cc~ )~0
 *   </pre>
 *   However, phrase queries are invalid inside a boolean expression with proximity constraints:
 *   <pre>
 *     ( +a "b c" )~0
 *   </pre>
 * </p>
 *
 * <h3>Datatype</h3>
 *
 * <p>
 * Some terms need to be analyzed in a specific way in order to be correctly
 * indexed and searched, e.g., numbers. For those terms to be searchable, the
 * keyword syntax provides a way to set how a query term should be analyzed.
 * Using a function-like syntax:
 * <pre>
 * datatype( ... )
 * </pre>
 * any query elements inside the parenthesis are processed using the datatype.
 * </p>
 * <p>
 * A mapping from a datatype label to an {@link org.apache.lucene.analysis.Analyzer}
 * is set thanks to configuration key
 * {@link com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys#DATATYPES_ANALYZERS}.
 * </p>
 * <p>
 * For example, I can search for documents where the values are integers ranging from
 * <code>5</code> to <code>10</code>
 * using the range query below:
 * <pre>
 * int( [ 5 TO 50 ] )
 * </pre>
 * The keyword parser in that example is configured to use
 * {@link com.sindicetech.siren.analysis.IntNumericAnalyzer}.
 * for the datatype <code>int</code>.
 * </p>
 * <p>
 * The top level node of a twig query is by default set to use the datatype
 * {@link com.sindicetech.siren.util.JSONDatatype#JSON_FIELD}. Any query elements
 * which is not wrapped in a custom datatype uses the datatype
 * {@link com.sindicetech.siren.util.XSDDatatype#XSD_STRING}.
 * </p>
 *
 */
package com.sindicetech.siren.qparser.keyword;


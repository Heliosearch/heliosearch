/**
 * A JSON query parser implemented with Jackson and the Lucene's Flexible Query
 * Parser.
 *
 * <h2>Query Parser Syntax</h2>
 *
 * <p>
 * A JSON Query is composed of one top-level object, which can be either:
 * <ul>
 * <li>a node (<code>node</code>) object that represents a
 * {@link NodeBooleanQuery} or a {@link NodePrimitiveQuery}; or
 * <li>a twig (<code>twig</code>) object that represents a {@link TwigQuery}; or
 * <li>a boolean (<code>boolean</code>) object that can represent a
 * {@link BooleanQuery} or a {@link BooleanSpanQuery} of node and twig query objects.
 * </ul>
 * </p>
 *
 * <p>
 * SIREn provides two implementations of the JSON query syntax:
 * <ul>
 *   <li>{@link com.sindicetech.siren.qparser.tree.ExtendedTreeQueryParser}, and</li>
 *   <li>{@link com.sindicetech.siren.qparser.tree.ConciseTreeQueryParser} which is an extension of the original JSON query
 *   parser for the concise tree model. This work exclusively in combination with the
 *   {@link com.sindicetech.siren.analysis.ConciseJsonTokenizer}.</li>
 * </ul>
 * </p>
 *
 * <h3>Node Boolean Query Expression</h3>
 *
 * <p>
 * The node and twig query object are composed of node boolean query expressions.
 * A node boolean query is a boolean combination of terms and phrases that will match
 * the content of one single node.
 * A node boolean query expression must follow the syntax supported by the
 * {@link KeywordQueryParser}, with the twig query syntax being disabled.
 * Examples of appropriately formatted keyword
 * queries can be found in the <a
 * href="{@docRoot}/com/sindicetech/siren/qparser/keyword/package-summary.html#package_description">
 * keyword query syntax documentation</a>.
 * </p>
 *
 * <h3>Node</h3>
 *
 * <p>
 * A node query matches a single node in the tree. A node object is always prefixed by the field name
 * "node" and follows the schema:
 *
 * <pre>
 *   "node" : {
 *
 *     "query" : String,    // Node boolean query expression - Mandatory
 *
 *     "level" : int,       // Node level constraint - Optional
 *
 *     "range" : [int, int] // Node range constraint - Optional
 *
 *   }
 * </pre>
 *
 * The "query" attribute is mandatory and must include a valid node boolean query expression.
 * The "level" and "range" attributes are optional. If there are not defined, the query will try to
 * match any node in the tree.
 * </p>
 *
 * <h4>Node Extension for the Concise Model</h4>
 *
 * <p>
 * The concise tree query syntax extends the node object with the "attribute" property in order to assign an
 * attribute label constraint to the node query. The query parser will automatically prepend the attribute name to all
 * the query terms. The string value of the "attribute" property is not analyzed and will be used as is.
 *
 * <pre>
 *   "node" : {
 *
 *     "query" : String,     // Node boolean query expression - Mandatory
 *
 *     "attribute" : String, // Attribute label - Optional
 *
 *     "level" : int,        // Node level constraint - Optional
 *
 *     "range" : [int, int]  // Node range constraint - Optional
 *
 *   }
 * </pre>
 *
 * </p>
 *
 * <p>
 * If no attribute property is specified, the query parser will not alter the query terms. In this case, you must ensure
 * that the {@link com.sindicetech.siren.analysis.filter.PathEncodingFilter} has been configured to preserve and generate
 * the original tokens, i.e., tokens before the prepending of the path information.
 * </p>
 *
 * <h3>Twig</h3>
 *
 * <p>
 * A twig query matches a root node with a boolean combination of one or more child and descendant nodes.
 * A twig object is always prefixed by the field name "twig" and follows the
 * schema:
 *
 * <pre>
 *   "twig" : {
 *
 *     "root"  : String,         // Node boolean query expression - Optional
 *
 *     "level" : int,            // Node level constraint - Optional
 *
 *     "range" : [int, int]      // Node range constraint - Optional
 *
 *     "child" : [ Clause ]      // Array of child clauses - Optional
 *
 *     "descendant" : [ Clause ] // Array of descendant clauses - Optional
 *
 *   }
 * </pre>
 * </p>
 *
 * <p>
 * The "root" attribute represents the query that must match the root node. It must include a valid node boolean
 * query expression. The "root" attribute is optional. If no root is defined, the query will try to match any root
 * node satisfying the other constraints, i.e., level, range, child or descendant.
 * </p>
 * <p>
 * The "level" and "range" attributes are optional. If no level is defined, the twig root will be associated to the
 * default level 1. If the twig is nested into another twig, its level will be derived from the parent twig and
 * will be overwritten.
 * </p>
 * <p>
 * The "child" and "descendant" attributes must contains an array of Clause object. The level of a child clause will be
 * automatically derived from the level of the twig root. On the other hand, the level of a descendant clause must
 * be specified in the Clause object.
 * <br/>
 * The "child" and "descendant" attributes are optional. It is possible to write a twig query without any child or
 * descendant clauses.
 * </p>
 * <p>
 * The root will automatically be assigned a default datatype {@link com.sindicetech.siren.util.JSONDatatype#JSON_FIELD}. If
 * a datatype is specified in the node boolean query expression of the root, it will have precedence over the default
 * datatype.
 * </p>
 *
 * <h4>Twig Extension for the Concise Model</h4>
 *
 * <p>
 * The concise tree query syntax extends the twig object by modifying the semantic of the "root" attribute. In the concise
 * model, it is assumed that all the intermediate nodes of the tree represent object's attributes. Therefore, the
 * root of a twig query will always be used to specify the label of an object's attribute. As a consequence, the "root"
 * attribute is used to specify an attribute label constraint on the root node. It must include a string value that will
 * be not analyzed and will be used as is.
 * </p>
 *
 * <h3>Boolean</h3>
 *
 * A boolean query matches a boolean combination of nodes. A boolean object is always prefixed by the field name
 * "boolean".
 *
 * <h4>Standard Boolean</h4>
 *
 * A boolean object follows the schema above:
 *
 * <pre>
 *   "boolean" : {
 *
 *     "clause"   : [ Clause ] // Array of clauses - Mandatory
 *
 *   }
 * </pre>
 *
 * It will be converted into a Lucene's {@link org.apache.lucene.search.BooleanQuery} if it occurs at the root of
 * the query object.
 *
 * <h4>Boolean with Proximity</h4>
 *
 * <p>
 * It is possible to create boolean expressions that must satisfy some proximity constraints, i.e., that will restrict
 * node matches based on their position. As with normal boolean expressions, you can nest them arbitrarily. These
 * proximity-aware boolean expressions follow the schema:
 *
 * <pre>
 *   "boolean" : {
 *
 *     "slop"     : int,        // Mandatory
 *
 *     "inOrder"  : boolean,    // Mandatory
 *
 *     "clause"   : [ Clause ]  // Array of clauses - Mandatory
 *
 *   }
 * </pre>
 * </p>
 *
 * <p>
 * The "slop" attribute can be used to specify the slop, or maximum distance allowed between nodes.
 * The slop constraint is applicable to the required clauses, but also to the optional clauses if the boolean expression
 * contains at least one required and one optional clause.
 * </p>
 *
 * <p>
 * The "inOrder"
 * attribute can be used to indicate that the nodes should appear in order. However, when an order
 * constraint is specified, it is only applicable to the required clauses, i.e., the optional and excluded clauses will
 * match irrespective of their position.
 * </p>
 *
 * <p>
 *   For more information about proximity-aware boolean query, please refer to
 *   {@link com.sindicetech.siren.search.spans.BooleanSpanQuery}.
 * </p>
 *
 * <h3>Clause</h3>
 *
 * <p>
 * A clause object defines a boolean unary operator associated to either a node object, a twig object, a boolean or a
 * variable object. There are different types of clause objects, each one following a slightly different
 * schema: boolean, child, descendant.
 * </p>
 *
 * <p>
 * All the clause object can have a "occur" attribute. The attribute can be used to specify the unary boolean
 * operator ("MUST", "MUST_NOT" or "SHOULD") that will be applied to the clause. The "occur" attribute is optional,
 * and if not specified, a default operator will be used. The default operator to be used depends on the configuration
 * of the {@link com.sindicetech.siren.qparser.tree.ExtendedTreeQueryParser}. See
 * {@link JsonQueryParser#setDefaultOperator(StandardQueryConfigHandler.Operator)}
 * to configure the default operator.
 *
 * <h4>Boolean Clause</h4>
 *
 * <p>
 * A boolean clause object follows the schema:
 *
 * <pre>
 *   {
 *
 *     // Boolean operator (MUST, MUST_NOT, SHOULD) - Optional
 *     "occur" : String,
 *
 *     // With either a node object
 *     "node"  : { ... },
 *
 *     // Or a twig object
 *     "twig" : { ... },
 *
 *     // Or a boolean object
 *     "boolean" : { ... },
 *
 *   }
 * </pre>
 * </p>
 *
 * <h4>Child Clause</h4>
 *
 * <p>
 * A child clause object follows the schema:
 *
 * <pre>
 *   {
 *
 *     // Boolean operator (MUST, MUST_NOT, SHOULD) - Optional
 *     "occur" : String,
 *
 *     // With either a node object
 *     "node"  : { ... },
 *
 *     // Or a twig object
 *     "twig" : { ... },
 *
 *     // Or a variable object
 *     "variable" : {},
 *
 *     // Or a boolean object
 *     "boolean" : { ... },
 *
 *   }
 * </pre>
 * </p>
 *
 * <p>
 * Variable object is currently supported only in a Twig child. Its occur attribute will automatically be set to SHOULD.
 * The variable value should be an empty object {}, the value is currently ignored.
 * </p>
 *
 * <h4>Descendant Clause</h4>
 *
 * <p>
 * A descendant clause object follows the schema:
 *
 * <pre>
 *   {
 *
 *     // Boolean operator (MUST, MUST_NOT, SHOULD) - Optional
 *     "occur" : String,
 *
 *     // With either a node object
 *     "node"  : { ... },
 *
 *     // Or a twig object
 *     "twig" : { ... },
 *
 *     // Or a boolean object
 *     "boolean" : { ... },
 *
 *     // Mandatory
 *     "level" : int
 *
 *   }
 * </pre>
 * </p>
 *
 * <p>
 * The "level" parameter is mandatory for descendant clauses and defines
 * the relative level of the descendant clauses with respect to the twig root level.
 * </p>
 *
 * <h2>Query Examples</h2>
 *
 * <h3>Node query</h3>
 *
 * <p>
 * Match all the documents with one node containing the phrase
 * "Marie Antoinette"
 *
 * <pre>
 * {
 *   "node" : {
 *     "query" : "\"Marie Antoinette\""
 *   }
 * }
 * </pre>
 * </p>
 *
 * <p>
 * If you are using the concise model and the concise tree query parser, you can ask the following query that matches all
 * the documents with one object with an attribute 'title' containing the phrase "Marie Antoinette"
 *
 * <pre>
 * {
 *   "node" : {
 *     "attribute" : "title",
 *     "query" : "\"Marie Antoinette\""
 *   }
 * }
 * </pre>
 * </p>
 *
 * <h3>Twig query</h3>
 *
 * <p>
 * Match all the documents with one node containing the term "genre" and with
 * a child node containing the term "Drama".
 *
 * <pre>
 * {
 *   "twig" : {
 *     "root" : "genre",
 *     "child" : [ {
 *       "occur" : "MUST",
 *       "node" : {
 *         "query" : "Drama"
 *       }
 *     } ]
 *   }
 * }
 * </pre>
 *
 * Such a twig query is the basic building block to query to a particular
 * field name of a JSON object. The field name is always the root of the twig
 * query and the field value is defined as a child clause.
 * </p>
 *
 * <p>
 * More complex twig queries can be constructed by using nested twig queries
 * or using more than one child (or descendant) clause.
 *
 * <pre>
 * {
 *   "twig" : {
 *     "root" : "director",
 *     "child" : [ {
 *       "occur" : "MUST",
 *       "twig" : {
 *         "child" : [ {
 *           "occur" : "MUST",
 *           "twig" : {
 *             "root" : "last_name",
 *             "child" : [ {
 *               "occur" : "MUST",
 *               "node" : {
 *                 "query" : "Eastwood"
 *               }
 *             } ]
 *           }
 *         }, {
 *           "occur" : "MUST",
 *           "twig" : {
 *             "root" : "first_name",
 *             "child" : [ {
 *               "occur" : "MUST",
 *               "node" : {
 *                 "query" : "Clint"
 *               }
 *             } ]
 *           }
 *         } ]
 *       }
 *     } ]
 *   }
 * }
 * </pre>
 * </p>
 *
 * <p>
 * If you are using the concise model and the concise tree query parser, you can ask the same query as follows:
 *
 * <pre>
 * {
 *   "twig" : {
 *     "root" : "director",
 *     "child" : [ {
 *       "occur" : "MUST",
 *       "node" : {
 *         "attribute" : "last_name",
 *         "query" : "Eastwood"
 *       }
 *     },{
 *       "occur" : "MUST",
 *       "node" : {
 *         "attribute" : "first_name",
 *         "query" : "Clint"
 *       }
 *     } ]
 *   }
 * }
 * </pre>
 *
 * Such a twig query is the basic building block to query to a particular
 * field name of a JSON object. The field name is always the root of the twig
 * query and the field value is defined as a child clause.
 * </p>
 *
 * <h3>Boolean Query</h3>
 *
 * <p>
 * Node and twig queries can be combined freely using the boolean query object
 * to create a boolean combination of node and twig queries.
 *
 * <pre>
 * {
 *   "boolean" : [ {
 *     "occur" : "MUST",
 *     "twig" : {
 *       "root" : "genre",
 *       "child" : [ {
 *         "occur" : "MUST",
 *         "node" : {
 *           "query" : "Drama"
 *         }
 *       } ]
 *     }
 *   }, {
 *     "occur" : "MUST",
 *     "twig" : {
 *       "root" : "year",
 *       "child" : [ {
 *         "occur" : "MUST",
 *         "node" : {
 *           "query" : "2010"
 *         }
 *       } ]
 *     }
 *   } ]
 * }
 * </pre>
 * </p>
 *
 * <h3>Proximity-Aware Boolean Query</h3>
 *
 * <p>
 * In this example, a twig query has one proximity-aware boolean query as a child clause. This proximity-aware
 * boolean query specifies that we must have one node that match "bbb" and an optional node that must be not further
 * than one node away in order to be counted in the scoring. This optional node is itself a proximity-aware
 * boolean query specifying that we must have two consecutive nodes, the first one matching "aaa" and the second one
 * matching "ccc".
 *
 * <pre>
 * {
 *   "twig" : {
 *     "child" : [ {
 *       "occur" : "MUST",
 *       "boolean" : {
 *         "slop" : 1,
 *         "clause" : [ {
 *           "occur" : "SHOULD",
 *           "boolean" : {
 *             "inOrder" : true,
 *             "clause" : [ {
 *               "occur" : "MUST",
 *               "node" : {
 *                 "query" : "aaa"
 *               }
 *             }, {
 *               "occur" : "MUST",
 *               "node" : {
 *                 "query" : "ccc"
 *               }
 *             } ]
 *           }
 *         }, {
 *           "occur" : "MUST",
 *           "node" : {
 *             "query" : "bbb"
 *           }
 *         } ]
 *       }
 *     } ]
 *   }
 * }
 * </pre>
 * </p>
 *
 * <h2>Query Builder DSL</h2>
 *
 * The package provides also a simple query builder to easily create JSON
 * queries programmatically. See {@link com.sindicetech.siren.qparser.tree.dsl.QueryBuilder}.
 *
 */
package com.sindicetech.siren.qparser.tree;


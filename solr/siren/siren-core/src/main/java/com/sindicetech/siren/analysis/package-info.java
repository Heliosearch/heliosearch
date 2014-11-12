/**
 * Analyzer for indexing JSON content.
 *
 * <h2>Introduction</h2>
 *
 * This package extends the Lucene's analysis API to provide support for
 * parsing and indexing JSON content. For an introduction to Lucene's analysis
 * API, see the {@link org.apache.lucene.analysis} package documentation.
 *
 *
 * <h2>Overview of the API</h2>
 *
 * This package contains concrete components
 * ({@link org.apache.lucene.util.Attribute}s,
 * {@link org.apache.lucene.analysis.Tokenizer}s and
 * {@link org.apache.lucene.analysis.TokenFilter}s) for analyzing different
 * JSON content.
 * <p>
 * It also provides a pre-built JSON analyzer
 * {@link com.sindicetech.siren.analysis.ExtendedJsonAnalyzer} that you can use to get
 * started quickly.
 * <p>
 * It also contains a number of
 * {@link com.sindicetech.siren.analysis.NumericAnalyzer}s that are used for
 * supporting datatypes.
 * <p>
 * The SIREn's analysis API is divided into several packages:
 * <ul>
 * <li><b>{@link com.sindicetech.siren.analysis.attributes}</b> contains a number of
 * {@link org.apache.lucene.util.Attribute}s that are used to add metadata
 * to a stream of tokens.
 * <li><b>{@link com.sindicetech.siren.analysis.filter}</b> contains a number of
 * {@link org.apache.lucene.analysis.TokenFilter}s that alter incoming tokens.
 * </ul>
 *
 * <h2>JSON Analyzer</h2>
 *
 * <p>
 * SIREn provides two different json tokenizers to parse and convert JSON data into
 * a node-labelled tree model:
 * <ul>
 *   <li>The {@link com.sindicetech.siren.analysis.ExtendedJsonTokenizer} converts JSON data into a tree model.</li>
 *   <li>The {@link com.sindicetech.siren.analysis.ConciseJsonTokenizer} converts JSON data into a concise tree model.</li>
 * </ul>
 * The conversion is performed in a streaming mode during the parsing.
 *</p>
 *
 * <p>
 * The tokenizer traverses the JSON tree using a depth-first search approach.
 * During the traversal of the tree, the tokenizer increments the dewey code
 * (i.e., node label) whenever an object, an array, a field or a value
 * is encountered. The tokenizer attaches to any token generated the current
 * node label using the
 * {@link com.sindicetech.siren.analysis.attributes.NodeAttribute}.
 * </p>
 *
 * <h3>JSON Datatypes</h3>
 *
 * The tokenizer attaches also a datatype metadata to any token generated using
 * the {@link com.sindicetech.siren.analysis.attributes.DatatypeAttribute}.
 * A datatype specifies the type of the data a node contains. By default, the
 * tokenizer differentiates five datatypes in the JSON syntax:
 *
 * <ul>
 * <li> {@link com.sindicetech.siren.util.XSDDatatype#XSD_STRING}
 * <li> {@link com.sindicetech.siren.util.XSDDatatype#XSD_LONG}
 * <li> {@link com.sindicetech.siren.util.XSDDatatype#XSD_DOUBLE}
 * <li> {@link com.sindicetech.siren.util.XSDDatatype#XSD_BOOLEAN}
 * <li> {@link com.sindicetech.siren.util.JSONDatatype#JSON_FIELD}
 * </ul>
 *
 * The datatype metadata is used to perform an appropriate analysis of the
 * content of a node. Such analysis is performed by the
 * {@link com.sindicetech.siren.analysis.filter.DatatypeAnalyzerFilter}. The
 * analysis of each datatype can be configured freely by the user using the
 * method
 * {@link com.sindicetech.siren.analysis.ExtendedJsonAnalyzer#registerDatatype(char[], org.apache.lucene.analysis.Analyzer)}.
 *
 * <h3>Custom Datatypes</h3>
 *
 * Custom datatypes can also be used thanks to a specific annotation in the JSON object.
 * The schema of the annotation is the following:
 * Custom datatypes can also be used thanks to a specific annotation in the JSON object.
 * The datatype annotation follows the above schema, with `&lt;LABEL&gt` being a string which represents the name of the
 * datatype to be assigned to the value, and `&lt;VALUE&gt` is a string representing the value.
 *
 * <pre>
 * {
 *   "_datatype_" : &lt;LABEL&gt;,
 *   "_value_" : &lt;VALUE&gt;
 * }
 * </pre>
 *
 * This annotation does not have influence on the label of the value node.
 * For example, the label (i.e., <code>0.0</code>) to the value <code>b</code> below:
 * <pre>
 * {
 *   "a" : "b"
 * }
 * </pre>
 * is the same for the value <code>b</code> with a custom datatype:
 * <pre>
 * {
 *   "a" : {
 *     "_datatype_" : "my datatype",
 *     "_value_" : "b"
 *   }
 * }
 * </pre>
 *
 * <h3>Trailing Commas</h3>
 *
 * The tokenizer allow trailing commas at the end of an array or an object,
 * although this is not possible by the <a href="http://www.json.org">JSON grammar</a>.
 * The reason is that this simplifies the code of the JSON scanner.
 *
 * For example, the following is accepted by our implementation, but not by the grammar:
 *
 * <pre>
 * { "a" : "b" , }
 * </pre>
 *
 * and
 *
 * <pre>
 * { "a" : [ "b" , "c" , ] }
 * </pre>
 *
 * <h2>Communication with the Posting Writer</h2>
 *
 * The Lucene's
 * {@link org.apache.lucene.analysis.tokenattributes.PayloadAttribute payload}
 * interface is used by SIREn to encode information such as the node label and
 * the position of the token. This payload is then decoded by the
 * {@link com.sindicetech.siren.index index API} and encoded back into the node-based
 * inverted index data structure.
 *
 */
package com.sindicetech.siren.analysis;


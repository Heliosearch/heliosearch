/**
 * Token's attributes used during JSON indexing
 *
 * <h2>Introduction</h2>
 *
 * This package contains a number of token's attributes that are used to attach
 * metadata to a token such as:
 * <ul>
 * <li> {@link com.sindicetech.siren.analysis.attributes.NodeAttribute} stores
 * the dewey code of the node from which this token comes from.
 * <li> {@link com.sindicetech.siren.analysis.attributes.PositionAttribute} stores
 * the position of the token relative to a node.
 * <li> {@link com.sindicetech.siren.analysis.attributes.NodeNumericTermAttribute}
 * stores a numeric value and is used by
 * {@link com.sindicetech.siren.analysis.NumericTokenizer} for indexing numeric
 * values.
 * <li> {@link com.sindicetech.siren.analysis.attributes.DatatypeAttribute} stores
 * the datatype of the node from which this token comes from.
 * </ul>
 *
 */
package com.sindicetech.siren.analysis.attributes;



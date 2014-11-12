/**
 * Abstraction over the encoding and decoding of the node-based inverted index
 * data structure.
 *
 * <h2>Introduction</h2>
 *
 * This package contains the low-level API for encoding and decoding
 * node information from the inverted index data structure. For an introduction
 * to Lucene's index API, see the {@link org.apache.lucene.index} package
 * documentation.
 *
 * <h2>Overview of the API</h2>
 *
 * This package defines the interface
 * {@link com.sindicetech.siren.index.DocsNodesAndPositionsEnum} to iterates over
 * the list of document identifiers, node frequencies, node labels, term
 * frequencies and term positions for a term.
 * <p>
 * It also provides a number of extensions that are used:
 * <ul>
 * <li> to filter entries based on node constraints (level or interval
 * constraints);
 * <li> to iterate over multiple segments during query processing or merging.
 * </ul>
 * <p>
 * The SIREn's index API is composed of two sub-packages:
 * <ul>
 * <li><b>{@link com.sindicetech.siren.index.codecs.block}</b> defines the abstract
 * API for encoding and decoding block-based posting formats.
 * <li><b>{@link com.sindicetech.siren.index.codecs.siren10}</b> contains the
 * implementation of the SIREn block-based posting format.
 * </ul>
 *
 */
package com.sindicetech.siren.index;


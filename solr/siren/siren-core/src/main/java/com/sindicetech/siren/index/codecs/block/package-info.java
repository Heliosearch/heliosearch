/**
 * Abstraction over the encoding and decoding of the block-based posting
 * format.
 *
 * <h2>Introduction</h2>
 *
 * This package contains the abstract API for encoding
 * ({@link com.sindicetech.siren.index.codecs.block.BlockIndexOutput}) and decoding
 * ({@link com.sindicetech.siren.index.codecs.block.BlockIndexInput})
 * block-based posting format. It also includes algorithms for compressing
 * and decompressing blocks of bytes.
 *
 * <h2>Block-Based Posting Format</h2>
 *
 * The block-based posting format encodes a posting list as a sequence of
 * blocks. A block is composed of an header, i.e., metadata, and some content,
 * i.e., bytes array. While the content of a block can be anything, it
 * usually contains a sequence of integers. In certain cases it can be composed
 * by multiple blocks of integers, for example to create interleaved blocks.
 * The size of a block can be either variable or fixed.
 *
 * <h3>Block Compression</h3>
 *
 * A {@link com.sindicetech.siren.index.codecs.block.BlockCompressor} compresses a
 * list of integers into a byte array in one batch.
 * A {@link com.sindicetech.siren.index.codecs.block.BlockIndexOutput} must ensure
 * that the given byte array is large enough for hosting the compressed data.
 * The method {@link com.sindicetech.siren.index.codecs.block.BlockCompressor#maxCompressedSize(int)}
 * can be used to estimate the maximum size of a compressed block of values.
 *
 * <p>
 *
 * A {@link com.sindicetech.siren.index.codecs.block.BlockDecompressor} decompresses
 * a compressed byte array into a list of integers in one batch.
 * A {@link com.sindicetech.siren.index.codecs.block.BlockIndexInput} must ensure
 * that the given integer array is large enough for hosting the uncompressed data.
 *
 * <p>
 *
 * Two block compression algorithms are implemented:
 * <ul>
 * <li> Variable Integer: encodes integers using the variable integer encoding
 * technique. It is very simple and provides a relatively efficient compression.
 * However, the compression ratio is not very good, especially for node labels
 * and term positions.
 * <li> Adaptive Frame Of Reference: encodes frames of integers using
 * highly-optimised routines. Its implementation is relatively complex but it
 * provides the best balance between compression ratio, compression speed and
 * decompression speed.
 * </ul>
 *
 * <h3>Concurrent Access</h3>
 *
 * During the creation of a new index segment, terms are processed sequentially.
 * This ensures that:
 * <ul>
 * <li> there is no concurrent access of the same
 * {@link com.sindicetech.siren.index.codecs.block.BlockIndexOutput} instance; and
 * <li> there is no concurrent encoding of multiple blocks.
 * </ul>
 *
 * During query processing, multiple terms are processed in parallel. The same
 * {@link com.sindicetech.siren.index.codecs.block.BlockIndexInput} will be used
 * to decode multiple postings list. Safe concurrent access of the index files
 * is ensured only if a different
 * {@link com.sindicetech.siren.index.codecs.block.BlockIndexInput.BlockReader}
 * is used for each postings list. The method
 * {@link com.sindicetech.siren.index.codecs.block.BlockIndexInput#getBlockReader()}
 * provides a
 * {@link com.sindicetech.siren.index.codecs.block.BlockIndexInput.BlockReader}
 * which contains a clone of the underlying
 * {@link org.apache.lucene.store.IndexInput}.
 *
 */
package com.sindicetech.siren.index.codecs.block;


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

package com.sindicetech.siren.index.codecs.siren10;

import org.apache.lucene.codecs.*;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsReader;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * Abstraction over the SIREn 1.0 postings format.
 *
 * <p>
 *
 * Subclasses must provide a pre-configured instance of the
 * {@link Siren10BlockStreamFactory}.
 */
public abstract class Siren10PostingsFormat extends PostingsFormat {

  /**
   * Filename extension for document identifiers and node frequencies.
   */
  public static final String DOC_EXTENSION = "doc";

  /**
   * Filename extension for nodes and term frequencies.
   */
  public static final String NOD_EXTENSION = "nod";

  /**
   * Filename extension for positions.
   */
  public static final String POS_EXTENSION = "pos";

  /**
   * Filename extension for skip data.
   */
  public static final String SKIP_EXTENSION = "skp";

  /**
   * Fixed block size, number of document identifiers encoded in
   * a single compressed block.
   */
  static final int DEFAULT_POSTINGS_BLOCK_SIZE = 32;

  protected final int blockSize;

  public Siren10PostingsFormat(final String name) {
    this(name, DEFAULT_POSTINGS_BLOCK_SIZE);
  }

  /**
   * Create a SIREn 1.0 posting format.
   * <p>
   * The block size parameter is used only during indexing.
   */
  public Siren10PostingsFormat(final String name, final int blockSize) {
    super(name);
    this.blockSize = blockSize;
  }

  protected abstract Siren10BlockStreamFactory getFactory();

  @Override
  public FieldsConsumer fieldsConsumer(final SegmentWriteState state)
  throws IOException {
    final PostingsWriterBase postingsWriter = new Siren10PostingsWriter(state,
      this.getFactory());

    boolean success = false;
    try {
      final FieldsConsumer ret = new BlockTreeTermsWriter(state, postingsWriter,
        BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE,
        BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
      success = true;
      return ret;
    }
    finally {
      if (!success) {
        postingsWriter.close();
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(final SegmentReadState state)
  throws IOException {
    final PostingsReaderBase postingsReader = new Siren10PostingsReader(state.directory,
      state.segmentInfo, state.context, state.segmentSuffix,
      this.getFactory());

    boolean success = false;
    try {
      final FieldsProducer ret = new BlockTreeTermsReader(state.directory,
                                                    state.fieldInfos,
                                                    state.segmentInfo,
                                                    postingsReader,
                                                    state.context,
                                                    state.segmentSuffix,
                                                    state.termsIndexDivisor);
      success = true;
      return ret;
    }
    finally {
      if (!success) {
        postingsReader.close();
      }
    }
  }

}

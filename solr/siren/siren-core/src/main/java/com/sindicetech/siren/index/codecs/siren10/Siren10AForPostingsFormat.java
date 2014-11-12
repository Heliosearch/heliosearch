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

import com.sindicetech.siren.index.codecs.block.AForBlockCompressor;
import com.sindicetech.siren.index.codecs.block.AForBlockDecompressor;

/**
 * Implementation of the {@link Siren10PostingsFormat} based on AFOR.
 */
public class Siren10AForPostingsFormat extends Siren10PostingsFormat {

  public static final String NAME = "Siren10AFor";

  public Siren10AForPostingsFormat() {
    super(NAME);
  }

  /**
   * Create a SIREn 1.0 posting format with VInt codec
   * <p>
   * The block size parameter is used only during indexing.
   */
  public Siren10AForPostingsFormat(final int blockSize) {
    super(NAME, blockSize);
  }

  @Override
  protected Siren10BlockStreamFactory getFactory() {
    final Siren10BlockStreamFactory factory = new Siren10BlockStreamFactory(blockSize);
    factory.setDocsBlockCompressor(new AForBlockCompressor());
    factory.setFreqBlockCompressor(new AForBlockCompressor());
    factory.setNodBlockCompressor(new AForBlockCompressor());
    factory.setPosBlockCompressor(new AForBlockCompressor());
    factory.setDocsBlockDecompressor(new AForBlockDecompressor());
    factory.setFreqBlockDecompressor(new AForBlockDecompressor());
    factory.setNodBlockDecompressor(new AForBlockDecompressor());
    factory.setPosBlockDecompressor(new AForBlockDecompressor());
    return factory;
  }

}

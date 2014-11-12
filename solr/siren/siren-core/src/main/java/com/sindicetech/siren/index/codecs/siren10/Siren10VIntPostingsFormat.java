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

import com.sindicetech.siren.index.codecs.block.VIntBlockCompressor;
import com.sindicetech.siren.index.codecs.block.VIntBlockDecompressor;

/**
 * Implementation of the {@link Siren10PostingsFormat} based on VInt.
 */
public class Siren10VIntPostingsFormat extends Siren10PostingsFormat {

  public static final String NAME = "Siren10VInt";

  public Siren10VIntPostingsFormat() {
    super(NAME);
  }

  /**
   * Create a SIREn 1.0 posting format with VInt codec
   * <p>
   * The block size parameter is used only during indexing.
   */
  public Siren10VIntPostingsFormat(final int blockSize) {
    super(NAME, blockSize);
  }

  @Override
  protected Siren10BlockStreamFactory getFactory() {
    final Siren10BlockStreamFactory factory = new Siren10BlockStreamFactory(blockSize);
    factory.setDocsBlockCompressor(new VIntBlockCompressor());
    factory.setFreqBlockCompressor(new VIntBlockCompressor());
    factory.setNodBlockCompressor(new VIntBlockCompressor());
    factory.setPosBlockCompressor(new VIntBlockCompressor());
    factory.setDocsBlockDecompressor(new VIntBlockDecompressor());
    factory.setFreqBlockDecompressor(new VIntBlockDecompressor());
    factory.setNodBlockDecompressor(new VIntBlockDecompressor());
    factory.setPosBlockDecompressor(new VIntBlockDecompressor());
    return factory;
  }

}

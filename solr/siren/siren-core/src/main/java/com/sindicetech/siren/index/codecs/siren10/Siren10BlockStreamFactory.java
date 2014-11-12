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

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import com.sindicetech.siren.index.codecs.block.BlockCompressor;
import com.sindicetech.siren.index.codecs.block.BlockDecompressor;
import com.sindicetech.siren.index.codecs.block.BlockIndexInput;
import com.sindicetech.siren.index.codecs.block.BlockIndexOutput;

/**
 * This class creates {@link BlockIndexOutput} and {@link BlockIndexInput}
 * for the SIREn 1.0 postings format.
 */
public class Siren10BlockStreamFactory {

  private final int blockSize;

  private BlockCompressor docsBlockCompressor;
  private BlockDecompressor docsBlockDecompressor;
  private BlockCompressor freqBlockCompressor;
  private BlockDecompressor freqBlockDecompressor;
  private BlockCompressor nodBlockCompressor;
  private BlockDecompressor nodBlockDecompressor;
  private BlockCompressor posBlockCompressor;
  private BlockDecompressor posBlockDecompressor;

  public Siren10BlockStreamFactory(final int blockSize) {
    this.blockSize = blockSize;
  }

  public void setDocsBlockCompressor(final BlockCompressor compressor) {
    this.docsBlockCompressor = compressor;
  }

  public void setDocsBlockDecompressor(final BlockDecompressor decompressor) {
    this.docsBlockDecompressor = decompressor;
  }

  public void setFreqBlockCompressor(final BlockCompressor compressor) {
    this.freqBlockCompressor = compressor;
  }

  public void setFreqBlockDecompressor(final BlockDecompressor decompressor) {
    this.freqBlockDecompressor = decompressor;
  }

  public void setNodBlockCompressor(final BlockCompressor compressor) {
    this.nodBlockCompressor = compressor;
  }

  public void setNodBlockDecompressor(final BlockDecompressor decompressor) {
    this.nodBlockDecompressor = decompressor;
  }

  public void setPosBlockCompressor(final BlockCompressor compressor) {
    this.posBlockCompressor = compressor;
  }

  public void setPosBlockDecompressor(final BlockDecompressor decompressor) {
    this.posBlockDecompressor = decompressor;
  }

  public DocsFreqBlockIndexOutput createDocsFreqOutput(final Directory dir,
                                                       final String fileName,
                                                       final IOContext context)
  throws IOException {
    return new DocsFreqBlockIndexOutput(
      dir.createOutput(fileName, context),
      blockSize,
      docsBlockCompressor, freqBlockCompressor);
  }

  public DocsFreqBlockIndexInput openDocsFreqInput(final Directory dir,
                                                   final String fileName,
                                                   final IOContext context)
  throws IOException {
    return new DocsFreqBlockIndexInput(
      dir.openInput(fileName, context),
      docsBlockDecompressor, freqBlockDecompressor);
  }

  public NodBlockIndexOutput createNodOutput(final Directory dir,
                                             final String fileName,
                                             final IOContext context)
  throws IOException {
    return new NodBlockIndexOutput(
      dir.createOutput(fileName, context),
      blockSize,
      nodBlockCompressor);
  }

  public NodBlockIndexInput openNodInput(final Directory dir,
                                         final String fileName,
                                         final IOContext context)
  throws IOException {
    return new NodBlockIndexInput(
      dir.openInput(fileName, context),
      nodBlockDecompressor);
  }

  public PosBlockIndexOutput createPosOutput(final Directory dir,
                                             final String fileName,
                                             final IOContext context)
  throws IOException {
    return new PosBlockIndexOutput(
      dir.createOutput(fileName, context),
      blockSize,
      posBlockCompressor);
  }

  public PosBlockIndexInput openPosInput(final Directory dir,
                                           final String fileName,
                                           final IOContext context)
  throws IOException {
    return new PosBlockIndexInput(
      dir.openInput(fileName, context),
      posBlockDecompressor);
  }

}

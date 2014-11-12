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
package com.sindicetech.siren.index.codecs.block;

import java.io.IOException;

import org.junit.Test;

import com.sindicetech.siren.index.codecs.CodecTestCase;
import com.sindicetech.siren.index.codecs.block.VIntBlockCompressor;
import com.sindicetech.siren.index.codecs.block.VIntBlockDecompressor;
import com.sindicetech.siren.index.codecs.siren10.DocsFreqBlockIndexInput;
import com.sindicetech.siren.index.codecs.siren10.DocsFreqBlockIndexOutput;
import com.sindicetech.siren.index.codecs.siren10.Siren10BlockStreamFactory;
import com.sindicetech.siren.index.codecs.siren10.DocsFreqBlockIndexInput.DocsFreqBlockReader;
import com.sindicetech.siren.index.codecs.siren10.DocsFreqBlockIndexOutput.DocsFreqBlockWriter;

public class TestVariableIntCodec extends CodecTestCase {

  private DocsFreqBlockIndexOutput getIndexOutput(final int blockSize) throws IOException {
    final Siren10BlockStreamFactory factory = new Siren10BlockStreamFactory(blockSize);
    factory.setDocsBlockCompressor(new VIntBlockCompressor());
    factory.setFreqBlockCompressor(new VIntBlockCompressor());
    return factory.createDocsFreqOutput(directory, "test", newIOContext(random()));
  }

  private DocsFreqBlockIndexInput getIndexInput() throws IOException {
    final Siren10BlockStreamFactory factory = new Siren10BlockStreamFactory(0);
    factory.setDocsBlockDecompressor(new VIntBlockDecompressor());
    factory.setFreqBlockDecompressor(new VIntBlockDecompressor());
    return factory.openDocsFreqInput(directory, "test", newIOContext(random()));
  }

  public void testReadDoc() throws IOException {

    final DocsFreqBlockIndexOutput out = this.getIndexOutput(512);
    final DocsFreqBlockWriter writer = out.getBlockWriter();

    writer.setNodeBlockIndex(out.index());
    writer.setPosBlockIndex(out.index());
    for (int i = 0; i < 11777; i++) {
      if (writer.isFull()) {
        writer.flush();
      }
      writer.write(i);
    }

    writer.flush(); // flush remaining data
    out.close();

    final DocsFreqBlockIndexInput in = this.getIndexInput();
    final DocsFreqBlockReader reader = in.getBlockReader();

    reader.setNodeBlockIndex(in.index());
    reader.setPosBlockIndex(in.index());
    for (int i = 0; i < 11777; i++) {
      if (reader.isExhausted()) {
        reader.nextBlock();
      }
      assertEquals(i, reader.nextDocument());
    }

    in.close();
  }

  public void testReadDocAndFreq() throws IOException {

    final DocsFreqBlockIndexOutput out = this.getIndexOutput(512);
    final DocsFreqBlockWriter writer = out.getBlockWriter();

    writer.setNodeBlockIndex(out.index());
    writer.setPosBlockIndex(out.index());
    for (int i = 0; i < 11777; i++) {
      if (writer.isFull()) {
        writer.flush();
      }
      writer.write(i);
      writer.writeNodeFreq(random().nextInt(10) + 1);
    }

    writer.flush(); // flush remaining data
    out.close();

    final DocsFreqBlockIndexInput in = this.getIndexInput();
    final DocsFreqBlockReader reader = in.getBlockReader();

    reader.setNodeBlockIndex(in.index());
    reader.setPosBlockIndex(in.index());
    for (int i = 0; i < 11777; i++) {
      if (reader.isExhausted()) {
        reader.nextBlock();
      }
      assertEquals(i, reader.nextDocument());
      final int frq = reader.nextNodeFreq();
      assertTrue(frq > 0);
      assertTrue(frq <= 10);
    }

    in.close();
  }

  @Test
  public void testIntegerRange() throws Exception {
    this.doTestIntegerRange(1, 32, new VIntBlockCompressor(), new VIntBlockDecompressor());
  }

}

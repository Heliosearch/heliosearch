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

package com.sindicetech.siren.index.codecs;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sindicetech.siren.index.codecs.block.BlockCompressor;
import com.sindicetech.siren.index.codecs.block.BlockDecompressor;
import com.sindicetech.siren.util.SirenTestCase;

public abstract class CodecTestCase extends SirenTestCase {

  private static final int MIN_LIST_SIZE = 32768;

  protected Directory directory;

  @Override
  @Before
  public void setUp()
  throws Exception {
    super.setUp();
    directory = newDirectory();
  }

  @Override
  @After
  public void tearDown()
  throws Exception {
    directory.close();
    super.tearDown();
  }

  /**
   * The different block sizes to test
   */
  protected static final int[] BLOCK_SIZES = {32, 64, 128, 256, 512, 1024, 2048};

  protected static final Logger logger = LoggerFactory.getLogger(CodecTestCase.class);

  /**
   * Generate a random long value uniformly distributed between
   * <code>lower</code> and <code>upper</code>, inclusive.
   *
   * @param lower
   *            the lower bound.
   * @param upper
   *            the upper bound.
   * @return the random integer.
   * @throws IllegalArgumentException if {@code lower >= upper}.
   */
  protected long nextLong(final long lower, final long upper) {
    if (lower >= upper) {
      throw new IllegalArgumentException();
    }
    final double r = random().nextDouble();
    return (long) ((r * upper) + ((1.0 - r) * lower) + r);
  }

  private void doTest(final int[] values, final int blockSize,
                      final BlockCompressor compressor,
                      final BlockDecompressor decompressor)
  throws Exception {
    final BytesRef compressedData = new BytesRef(compressor.maxCompressedSize(blockSize));
    final IntsRef input = new IntsRef(blockSize);
    final IntsRef output = new IntsRef(blockSize);

    for (int i = 0; i < values.length; i += blockSize) {

      int offset = 0;

      // copy first block into the uncompressed data buffer
      for (int j = i; offset < blockSize && j < values.length; j++, offset++) {
        input.ints[offset] = values[j];
      }
      input.offset = 0;
      input.length = offset;

      // compress
      compressor.compress(input, compressedData);

      // decompress
      decompressor.decompress(compressedData, output);

      // check if they are equals
      for (int j = 0; j < input.length; j++) {
        assertEquals(input.ints[j], output.ints[j]);
      }
    }
  }

  public void doTestIntegerRange(final int minBits, final int maxBits, final int[] blockSizes,
                                 final BlockCompressor compressor,
                                 final BlockDecompressor decompressor)
  throws Exception {
    for (int i = minBits; i <= maxBits; i++) {
      // different length for each run
      final int length = TestUtil.nextInt(random(), MIN_LIST_SIZE, MIN_LIST_SIZE * 2);
      final int[] input = new int[length];

      final long min = i == 1 ? 0 : (1L << (i - 1));
      final long max = ((1L << i) - 1);

      // generate integers per frame of max 8 ints
      for (int j = 0; j < length; j += 8) {
        final int size = j + 8 < length ? 8 : length - j;
        // generate randomly a sequence of 8 zero integers, in order to force
        // frame-based compression to use instructions for different frame sizes
        final boolean bool = random().nextBoolean();
        int[] ints;
        if (bool) {
          ints = this.getRandomInteger(size, min, max);
        }
        else {
          ints = new int[size]; // initialised by default with 0
        }
        System.arraycopy(ints, 0, input, j, ints.length);
      }

      for (final int blockSize : blockSizes) {
//        logger.debug("Perform Integer Range Test: length = {}, bits = {}, block size = {}",
//          new Object[]{length, i, blockSize});
        this.doTest(input, blockSize, compressor, decompressor);
      }
    }
  }

  private int[] getRandomInteger(final int size, final long min, final long max) {
    final int[] ints = new int[size];
    for (int i = 0; i < size; i++) {
      ints[i] = (int) this.nextLong(min, max);;
    }
    return ints;
  }

  public void doTestIntegerRange(final int minBits, final int maxBits,
                                 final BlockCompressor compressor,
                                 final BlockDecompressor decompressor)
  throws Exception {
    this.doTestIntegerRange(minBits, maxBits, BLOCK_SIZES, compressor, decompressor);
  }

}

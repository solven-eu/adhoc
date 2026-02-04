/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.compression.packing;

import java.util.Arrays;

import eu.solven.adhoc.compression.dictionary.IIntArray;
import lombok.experimental.UtilityClass;
import me.lemire.integercompression.BitPacking;
import me.lemire.integercompression.Util;

/**
 * Entry-point to access packing algorithms. It may choose different packing strategy depending on the situation.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class PackedIntegers {
	private static final int BITS_PER_INT = 32;

	public static IIntArray doPack(int... input) {
		return doPack(false, input);
	}

	static IIntArray doPack(boolean forceFlexible, int... input) {
		// fastpackwithoutmask requires input to have size % 32
		int[] input32;

		if (input.length % BITS_PER_INT != 0) {
			input32 = Arrays.copyOf(input, BITS_PER_INT * (1 + input.length / BITS_PER_INT));
		} else {
			input32 = input;
		}

		// `fastpackwithoutmask` will pack 32 integers into this number of integers
		final int bits = Util.maxbits(input32, 0, input.length);

		if (bits == 0) {
			return new ZeroPackedIntegers(input.length);
		}

		int nbBlocks = input32.length / BITS_PER_INT;
		int[] output = new int[bits * nbBlocks];

		for (int i = 0; i < nbBlocks; i++) {
			BitPacking.fastpackwithoutmask(input32, BITS_PER_INT * i, output, bits * i, bits);
		}

		// BEWARE Should we sometimes prefer prefer force packing into a singleChunk?
		// For instance, if bits==7, we may prefer storing 4 ints per chunk (hence consuming 4*7=28 bits, and losing 2
		// bits)
		if (!forceFlexible && SingleChunkPackedIntegers.isCompatible(bits)) {
			return SingleChunkPackedIntegers.builder().bitsPerInt(bits).intsLength(input.length).holder(output).build();
		} else {
			return FlexiblePackedIntegers.builder().bitsPerInt(bits).intsLength(input.length).holder(output).build();
		}
	}

	@Deprecated(since = "Used only for benchmarks")
	static IIntArray asFlexible(IIntArray flexible) {
		if (!(flexible instanceof SingleChunkPackedIntegers singleChunk)) {
			throw new IllegalArgumentException("Expected %s".formatted(SingleChunkPackedIntegers.class.getName()));
		}

		return FlexiblePackedIntegers.builder()
				.bitsPerInt(singleChunk.bitsPerInt)
				.intsLength(singleChunk.intsLength)
				.holder(singleChunk.holder)
				.build();
	}
}

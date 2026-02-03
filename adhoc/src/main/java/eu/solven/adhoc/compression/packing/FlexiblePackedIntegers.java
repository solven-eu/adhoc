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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import eu.solven.adhoc.compression.IIntArray;
import lombok.Builder;
import lombok.NonNull;
import me.lemire.integercompression.BitPacking;
import me.lemire.integercompression.Util;

/**
 * Used to compress an `int[]` when all ints are known to be relatively small. `bits` represent the number of expressed
 * bits: all others are considered to be 0.
 * 
 * This is very flexible as it can handle any number of actual used bits.
 * 
 * @author Benoit Lacelle
 */
// TODO Add ability to read multiple entries, as done by `BitPacking`
public final class FlexiblePackedIntegers implements IIntArray {
	private static final int BITS_PER_INT = 32;

	// each `byte` provide 8 bits.
	// each `int` provide 32 bits.
	static final int BITS_PER_CHUNK = BITS_PER_INT;

	// number of lower bits expressed per integer (higher bits are all zeros)
	int bitsPerInt;
	// number of packed ints
	int intsLength;

	// length is `(length * bits) / chunkSize`
	@NonNull
	int[] holder;

	final int mask;

	@Builder
	@SuppressWarnings("PMD.UseVarargs")
	private FlexiblePackedIntegers(int bitsPerInt, int intsLength, int[] holder) {
		this.bitsPerInt = bitsPerInt;
		this.intsLength = intsLength;
		this.holder = holder;

		this.mask = ~0 >>> (BITS_PER_INT - bitsPerInt);

		if (bitsPerInt == 0) {
			throw new IllegalArgumentException(
					"Given bits==0, one should rely on %s".formatted(ZeroPackedIntegers.class.getName()));
		}
	}

	@Override
	public int length() {
		return intsLength;
	}

	@Override
	public void writeInt(int index, int value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int readInt(int index) {
		if (index < 0 || index >= intsLength) {
			throw new ArrayIndexOutOfBoundsException("index:%s >= length:%s".formatted(index, intsLength));
		}

		// maximum number of chunks amongst which the pack is dispatched
		// int nbChunks = 1 + (bits - 1) / chunkSize;

		int firstBitIndex = index * bitsPerInt;

		// Index of the first chunk holding relevant bits
		int firstChunkIndex = firstBitIndex / BITS_PER_CHUNK;
		// Number of bits to skip in the first chunk
		int firstChunkShift = firstBitIndex - firstChunkIndex * BITS_PER_CHUNK;

		// Needed bits may be split on multiple chunks
		int bitsLeft = bitsPerInt;

		// We need to append the writes
		int shiftWrite = 0;

		int output = 0;

		// Initial block
		{
			// We may need to skip some bits on the first chunk
			int shiftRead = firstChunkShift;

			int maskRead = mask << shiftRead;

			output |= holder[firstChunkIndex] & maskRead >>> shiftRead;

			int nbWritten = BITS_PER_CHUNK - shiftRead;
			bitsLeft -= nbWritten;
			shiftWrite += nbWritten;
		}

		// Read from next blocks
		// BEWARE We may read 1 or more blocks, depending on blocks size
		// For instance, if block is int, we're guaranteed to read at most one other block
		while (bitsLeft > 0) {
			firstChunkIndex++;

			int maskRead = mask >>> shiftWrite;

			output |= holder[firstChunkIndex] & maskRead << shiftWrite;

			// BEWARE We might write less bits if we read not all bits of next chunk
			int nbWritten = BITS_PER_CHUNK;
			bitsLeft -= nbWritten;
			shiftWrite += nbWritten;
		}

		return output;
	}

	@Override
	public String toString() {
		return toString(this);
	}

	public static IIntArray doPack(int... input) {
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
		if (SingleChunkPackedIntegers.isCompatible(bits)) {
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

	public static String toString(IIntArray intArray) {
		return IntStream.range(0, intArray.length())
				.map(intArray::readInt)
				.mapToObj(Integer::toString)
				.collect(Collectors.joining(", "));
	}
}

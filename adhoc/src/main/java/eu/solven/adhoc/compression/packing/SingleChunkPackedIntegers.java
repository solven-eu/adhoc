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

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import eu.solven.adhoc.compression.IIntArray;
import lombok.Builder;
import lombok.NonNull;

/**
 * Like {@link FlexiblePackedIntegers} but specialized when we know an int is encoded into a single chunk.
 * 
 * @author Benoit Lacelle
 */
// TODO Add ability to read multiple entries, as done by `BitPacking`
public final class SingleChunkPackedIntegers implements IIntArray {
	private static final int BITS_PER_INT = 32;

	// each `byte` provide 8 bits.
	// each `int` provide 32 bits.
	static final int BITS_PER_CHUNK = BITS_PER_INT;

	// number of lower bits expressed per integer (higher bits are all zeros)
	int bits;
	// number of packed ints
	int intsLength;

	// length is `(length * bits) / chunkSize`
	@NonNull
	int[] holder;

	private final int[] masks;

	/**
	 * 
	 * @param bits
	 *            number of bits per int
	 * @return true if SingleChunkPackedIntegers is compatible with this number of bits
	 */
	static boolean isCompatible(int bits) {
		// We accepts 1bit (max1), 2 bits (max=3), 4 bits (max=255), 8 bits(max=65535), 16 bits, 32 bits (max=-1)
		return BITS_PER_CHUNK % bits == 0;
	}

	@Builder
	@SuppressWarnings("PMD.UseVarargs")
	SingleChunkPackedIntegers(int bits, int intsLength, int[] holder) {
		this.bits = bits;
		this.intsLength = intsLength;
		this.holder = holder;

		int nbPerChunk = BITS_PER_CHUNK / bits;
		this.masks = new int[nbPerChunk];
		for (int shiftRead = 0; shiftRead < nbPerChunk; shiftRead++) {
			this.masks[shiftRead] = (0xFFFFFFFF >>> (BITS_PER_INT - bits)) << shiftRead * bits;
		}

		if (BITS_PER_CHUNK % bits != 0) {
			throw new IllegalArgumentException(
					"Requires bits=%s to be a divisor of %s".formatted(bits, BITS_PER_CHUNK));
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
		} else if (bits == 0) {
			return 0;
		}

		// maximum number of chunks amongst which the pack is dispatched
		// int nbChunks = 1 + (bits - 1) / chunkSize;

		int firstBitIndex = index * bits;

		// Index of the first chunk holding relevant bits
		int chunkIndex = firstBitIndex / BITS_PER_CHUNK;
		// Number of bits to skip in the first chunk
		int shiftRead = firstBitIndex - chunkIndex * BITS_PER_CHUNK;

		int maskRead = masks[index % masks.length];

		int contrib = holder[chunkIndex] & maskRead;
		return contrib >>> shiftRead;
	}

	@Override
	public String toString() {
		return IntStream.range(0, length())
				.map(this::readInt)
				.mapToObj(Integer::toString)
				.collect(Collectors.joining(", "));
	}

}

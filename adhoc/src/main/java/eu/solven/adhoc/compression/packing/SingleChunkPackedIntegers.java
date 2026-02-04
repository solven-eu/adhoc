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

import eu.solven.adhoc.compression.dictionary.IIntArray;
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
	static final int BITSSHIFT_PER_INT = 5;

	// number of lower bits expressed per integer (higher bits are all zeros)
	int bitsPerInt;
	int bitsPerIntAsBitShift;
	// number of packed ints
	int intsLength;

	// length is `(length * bits) / chunkSize`
	@NonNull
	int[] holder;

	final int nbPerChunkMask;
	final int maskForFirstBits;
	final int chunkIndexShift;

	/**
	 * 
	 * @param bits
	 *            number of bits per int
	 * @return true if SingleChunkPackedIntegers is compatible with this number of bits
	 */
	static boolean isCompatible(int bits) {
		// We accepts 1bit (max1), 2 bits (max=3), 4 bits (max=255), 8 bits(max=65535), 16 bits, 32 bits (max=-1)
		return bits != 0 && BITS_PER_INT % bits == 0;
	}

	@Builder
	@SuppressWarnings({ "PMD.UseVarargs", "PMD.ArrayIsStoredDirectly" })
	SingleChunkPackedIntegers(int bitsPerInt, int intsLength, int[] holder) {
		if (!isCompatible(bitsPerInt)) {
			throw new IllegalArgumentException(
					"Requires bits=%s to be a divisor of %s".formatted(bitsPerInt, BITS_PER_INT));
		}

		this.bitsPerInt = bitsPerInt;
		this.intsLength = intsLength;
		this.holder = holder;

		this.nbPerChunkMask = BITS_PER_INT / bitsPerInt - 1;
		this.bitsPerIntAsBitShift = Integer.numberOfTrailingZeros(bitsPerInt);
		this.maskForFirstBits = 0xFFFFFFFF >>> (BITS_PER_INT - bitsPerInt);
		this.chunkIndexShift = (BITSSHIFT_PER_INT - bitsPerIntAsBitShift);
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

		// Number of bits to skip in the chunk
		// `index & nbPerChunkMask` does a modulo
		int shiftRead = (index & nbPerChunkMask) << bitsPerIntAsBitShift;

		// `index >>> chunkIndexShift` does a division
		return (holder[index >>> chunkIndexShift] & (maskForFirstBits << shiftRead)) >>> shiftRead;
	}

	@Override
	public String toString() {
		return FlexiblePackedIntegers.toString(this);
	}
}

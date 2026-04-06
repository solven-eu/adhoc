/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.dataframe.collection;

/**
 * Shared constants and index-arithmetic utilities used by {@link ChunkedList}, {@link LongChunkedList}, and
 * {@link DoubleChunkedList}.
 *
 * <p>
 * All three classes share the same head/tail storage layout and the same bit-arithmetic to map a flat index to a chunk
 * position. This class is the single source of truth for those computations.
 *
 * @author Benoit Lacelle
 */
final class ChunkedArrays {

	/** Default {@code log2(base)} — used when no initial capacity hint is provided. */
	static final int LOG2_BASE_DEFAULT = 7;

	/** Default head size: {@code 1 << LOG2_BASE_DEFAULT} = 128. */
	static final int BASE_DEFAULT = 1 << LOG2_BASE_DEFAULT;

	// private: utility class, not meant to be instantiated
	private ChunkedArrays() {
	}

	/**
	 * Selects the per-instance {@code log2Base} from a requested capacity.
	 *
	 * <p>
	 * The default is {@value #LOG2_BASE_DEFAULT} (base = {@value #BASE_DEFAULT}). A larger base is chosen only when
	 * {@code initialCapacity} fills more than 75 % of the next higher power of two — i.e.
	 * {@code initialCapacity * 4 > nextPow2 * 3} — because below that threshold the unconditionally pre-allocated head
	 * wastes more memory than the tail indirection it avoids. Below 75 %, a base of {@code nextPow2 / 4} is used
	 * instead to keep the head footprint small.
	 *
	 * <p>
	 * Examples: 0 or 128 → 7 (base 128), 300 → 7 (base 128), 384 → 9 (base 512, 75 % boundary), 510 → 9 (base 512), 512
	 * → 9 (base 512).
	 */
	@SuppressWarnings("checkstyle:MagicNumber")
	static int computeLog2Base(int initialCapacity) {
		if (initialCapacity <= BASE_DEFAULT) {
			return LOG2_BASE_DEFAULT;
		}
		// Ceiling log2: exact powers of two use their own exponent; all others round up.
		int log2Floor = 31 - Integer.numberOfLeadingZeros(initialCapacity);
		int nextPow2Log;
		if (Integer.highestOneBit(initialCapacity) == initialCapacity) {
			nextPow2Log = log2Floor;
		} else {
			nextPow2Log = log2Floor + 1;
		}
		int nextPow2 = 1 << nextPow2Log;
		if (4 * initialCapacity <= nextPow2 * 3) {
			// capacity in (prevPow2, 75% · nextPow2]: use nextPow2/4 to keep the head footprint small.
			return nextPow2Log - 2;
		}
		return nextPow2Log;
	}

	/**
	 * Returns the tail-chunk index for a given {@code unitIndex}: {@code floor(log2(unitIndex + 1))}.
	 *
	 * <p>
	 * Tail chunk {@code k} covers unit indices {@code [2ᵏ−1, 2ᵏ⁺¹−2]}.
	 */
	@SuppressWarnings("checkstyle:MagicNumber")
	static int tailChunkIndex(int unitIndex) {
		return 31 - Integer.numberOfLeadingZeros(unitIndex + 1);
	}
}

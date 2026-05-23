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

import java.util.Arrays;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import eu.solven.adhoc.collection.FrozenException;
import eu.solven.adhoc.collection.ICompactable;
import eu.solven.adhoc.collection.IFreezable;
import it.unimi.dsi.fastutil.doubles.AbstractDoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

/**
 * A primitive {@link DoubleList} backed by the same head/tail chunked layout as {@link ChunkedList}, avoiding boxing of
 * {@code double} values entirely.
 *
 * <p>
 * Storage layout (default base = 128, all arrays are {@code double[]}):
 *
 * <pre>
 * head (size 128):       indices   0 –  127
 * tail[0] (size 128):   indices 128 –  255
 * tail[1] (size 256):   indices 256 –  511
 * tail[k] (size 128·2ᵏ): …
 * </pre>
 *
 * <p>
 * See {@link ChunkedArrays} for the index-arithmetic shared with {@link ChunkedList} and {@link ChunkedLongList}.
 *
 * <p>
 * Specialized for append-only workloads: {@link #add(double)} (and {@link #add(int, double)} when
 * {@code index == size}) hits an append-cache fast path that bypasses the per-cell chunk arithmetic. Mid-list inserts
 * and removals work but invalidate the cache.
 *
 * @author Benoit Lacelle
 */
public class ChunkedDoubleList extends AbstractDoubleList implements IFreezable, ICompactable {

	private final int log2Base;
	private final int base;

	// non-final: compact() may replace with a trimmed copy
	private double @Nullable [] head;
	private double @Nullable [] @Nullable [] tail;
	// PMD.AvoidFieldNameMatchingMethodName: `size` is the idiomatic AbstractList field name; renaming would deviate
	// from the JDK pattern
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	private int size;

	// Append cache: chunk currently containing index `size` (the next append's destination), plus the global index
	// of that chunk's slot 0. See {@link ChunkedLongList} for the full invariant — same shape applies here.
	private double @Nullable [] appendChunk;
	private int appendChunkBase;

	// private: one-way freeze transition must not be bypassed by subclasses
	private boolean compacted;

	/** Creates an empty list with the default base size (128). The head array is allocated lazily on first write. */
	public ChunkedDoubleList() {
		this.log2Base = ChunkedArrays.LOG2_BASE_DEFAULT;
		this.base = ChunkedArrays.BASE_DEFAULT;
	}

	/**
	 * Creates an empty list pre-sizing the tail pointer array for up to {@code initialCapacity} elements. The base size
	 * is derived from the capacity via {@link ChunkedArrays#computeLog2Base}.
	 */
	public ChunkedDoubleList(int initialCapacity) {
		if (initialCapacity < 0) {
			throw new IllegalArgumentException("Negative initialCapacity: " + initialCapacity);
		}
		this.log2Base = ChunkedArrays.computeLog2Base(initialCapacity);
		this.base = 1 << log2Base;
		// head allocated lazily on first write
		if (initialCapacity > base) {
			int tailSlots = ChunkedArrays.tailChunkIndex((initialCapacity - base - 1) >> log2Base) + 1;
			tail = new double[tailSlots][];
		}
	}

	@Override
	public double getDouble(int index) {
		Objects.checkIndex(index, size);
		return readAt(index);
	}

	@Override
	public double set(int index, double k) {
		checkNotCompacted();
		Objects.checkIndex(index, size);
		double old = readAt(index);
		writeAt(index, k);
		// Cache stays valid: set() does not change `size`.
		return old;
	}

	/**
	 * Pure append. Hot path: bypasses the bounds check and {@code index == size} branch of {@link #add(int, double)} by
	 * reading the cached {@code appendChunk} directly.
	 */
	@Override
	public boolean add(double k) {
		checkNotCompacted();
		int s = size;
		double[] wc = appendChunk;
		if (wc != null && s - appendChunkBase < wc.length) {
			wc[s - appendChunkBase] = k;
			size = s + 1;
			return true;
		}
		appendSlow(k);
		return true;
	}

	@Override
	public void add(int index, double k) {
		checkNotCompacted();
		int s = size;
		Objects.checkIndex(index, s + 1);
		if (index == s) {
			double[] wc = appendChunk;
			if (wc != null && s - appendChunkBase < wc.length) {
				wc[s - appendChunkBase] = k;
				size = s + 1;
				return;
			}
			appendSlow(k);
			return;
		}
		// Mid-list insert: the chunk for the new `size` may differ from the cached one.
		appendChunk = null;
		ensureTailChunkFor(s);
		for (int i = s; i > index; i--) {
			writeAt(i, readAt(i - 1));
		}
		writeAt(index, k);
		size = s + 1;
	}

	// Slow path: ensure the destination chunk exists, write, then repoint the cache so consecutive appends stay
	// on the fast path. See {@link ChunkedLongList#appendSlow} for the same logic on `long`.
	private void appendSlow(double k) {
		int s = size;
		ensureTailChunkFor(s);
		writeAt(s, k);
		if (s < base) {
			appendChunk = head;
			appendChunkBase = 0;
		} else {
			int unitIndex = (s - base) >> log2Base;
			int chunkIndex = ChunkedArrays.tailChunkIndex(unitIndex);
			appendChunk = Objects.requireNonNull(Objects.requireNonNull(tail)[chunkIndex]);
			appendChunkBase = base << chunkIndex;
		}
		size = s + 1;
	}

	@Override
	public double removeDouble(int index) {
		checkNotCompacted();
		Objects.checkIndex(index, size);
		double old = readAt(index);
		for (int i = index; i < size - 1; i++) {
			writeAt(i, readAt(i + 1));
		}
		size--;
		appendChunk = null;
		return old;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public void clear() {
		checkNotCompacted();
		// Primitive arrays need no nulling — we only reset the logical size.
		size = 0;
		appendChunk = null;
	}

	// --- compact ---

	// CPD-OFF
	/**
	 * Shrinks all storage to exactly the space needed for the current elements, then marks this list as read-only.
	 *
	 * @see ChunkedList#compact()
	 */
	@Override
	public void compact() {
		if (size < base) {
			if (head != null && size < head.length) {
				head = Arrays.copyOf(head, size);
			}
		} else if (size > base && tail != null) {
			int adjusted = size - 1 - base;
			int unitIndex = adjusted >> log2Base;
			int k = ChunkedArrays.tailChunkIndex(unitIndex);
			int offset = ((unitIndex - (1 << k) + 1) << log2Base) + (adjusted & (base - 1));
			double[] chunk = Objects.requireNonNull(tail[k]);
			if (offset + 1 < chunk.length) {
				tail[k] = Arrays.copyOf(chunk, offset + 1);
			}
		}
		compacted = true;
		appendChunk = null;
	}

	@Override
	public boolean isFrozen() {
		return compacted;
	}

	private void checkNotCompacted() {
		if (compacted) {
			throw new FrozenException("DoubleChunkedList has been compacted and is frozen");
		}
	}

	// --- internal read/write ---

	private double readAt(int index) {
		if (index < base) {
			// callers guarantee `index < size`; head was allocated by a prior write
			return Objects.requireNonNull(head)[index];
		}
		int adjusted = index - base;
		int unitIndex = adjusted >> log2Base;
		int k = ChunkedArrays.tailChunkIndex(unitIndex);
		int offset = ((unitIndex - (1 << k) + 1) << log2Base) + (adjusted & (base - 1));
		return Objects.requireNonNull(Objects.requireNonNull(tail)[k])[offset];
	}

	private void writeAt(int index, double value) {
		if (index < base) {
			double[] localHead = head;
			if (localHead == null) {
				localHead = new double[base];
				head = localHead;
			}
			localHead[index] = value;
			return;
		}
		int adjusted = index - base;
		int unitIndex = adjusted >> log2Base;
		int k = ChunkedArrays.tailChunkIndex(unitIndex);
		int offset = ((unitIndex - (1 << k) + 1) << log2Base) + (adjusted & (base - 1));
		double[] chunk = Objects.requireNonNull(Objects.requireNonNull(tail)[k]);
		chunk[offset] = value;
	}

	private void ensureTailChunkFor(int index) {
		if (index < base) {
			return;
		}
		int adjusted = index - base;
		int unitIndex = adjusted >> log2Base;
		int k = ChunkedArrays.tailChunkIndex(unitIndex);
		if (tail == null) {
			tail = new double[k + 1][];
		} else if (k >= tail.length) {
			tail = Arrays.copyOf(tail, k + 1);
		}
		if (tail[k] == null) {
			tail[k] = new double[base << k];
		}
	}
	// CPD-ON
}

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

import eu.solven.adhoc.collection.FrozenException;
import eu.solven.adhoc.collection.IFrozen;
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
 * See {@link ChunkedArrays} for the index-arithmetic shared with {@link ChunkedList} and {@link LongChunkedList}.
 *
 * @author Benoit Lacelle
 */
public class DoubleChunkedList extends AbstractDoubleList implements IFrozen {

	private final int log2Base;
	private final int base;

	// non-final: compact() may replace with a trimmed copy
	private double[] head;
	private double[][] tail;
	// PMD.AvoidFieldNameMatchingMethodName: `size` is the idiomatic AbstractList field name; renaming would deviate
	// from the JDK pattern
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	private int size;

	// private: one-way freeze transition must not be bypassed by subclasses
	private boolean compacted;

	/**
	 * Modification count for fail-fast iterator support. fastutil's {@link AbstractDoubleList} does not inherit
	 * {@code modCount} from {@link java.util.AbstractList}, so we declare it here.
	 */
	protected int modCount;

	/** Creates an empty list with the default base size (128). The head array is allocated lazily on first write. */
	public DoubleChunkedList() {
		this.log2Base = ChunkedArrays.LOG2_BASE_DEFAULT;
		this.base = ChunkedArrays.BASE_DEFAULT;
	}

	/**
	 * Creates an empty list pre-sizing the tail pointer array for up to {@code initialCapacity} elements. The base size
	 * is derived from the capacity via {@link ChunkedArrays#computeLog2Base}.
	 */
	public DoubleChunkedList(int initialCapacity) {
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
		return old;
	}

	@Override
	public void add(int index, double k) {
		checkNotCompacted();
		Objects.checkIndex(index, size + 1);
		ensureTailChunkFor(size);
		for (int i = size; i > index; i--) {
			writeAt(i, readAt(i - 1));
		}
		writeAt(index, k);
		size++;
		modCount++;
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
		modCount++;
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
		modCount++;
	}

	// --- compact ---

	// CPD-OFF
	/**
	 * Shrinks all storage to exactly the space needed for the current elements, then marks this list as read-only.
	 *
	 * @return {@code this}, for chaining
	 * @see ChunkedList#compact()
	 */
	@SuppressWarnings("PMD.LooseCoupling")
	public DoubleChunkedList compact() {
		if (size < base) {
			if (head != null && head.length < size) {
				head = Arrays.copyOf(head, size);
			}
		} else if (size > base && tail != null) {
			int adjusted = size - 1 - base;
			int unitIndex = adjusted >> log2Base;
			int k = ChunkedArrays.tailChunkIndex(unitIndex);
			int lastOffset = ((unitIndex - (1 << k) + 1) << log2Base) + (adjusted & (base - 1));
			if (lastOffset + 1 < tail[k].length) {
				tail[k] = Arrays.copyOf(tail[k], lastOffset + 1);
			}
		}
		compacted = true;
		modCount++;
		return this;
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
			return head[index];
		}
		int adjusted = index - base;
		int unitIndex = adjusted >> log2Base;
		int k = ChunkedArrays.tailChunkIndex(unitIndex);
		int offset = ((unitIndex - (1 << k) + 1) << log2Base) + (adjusted & (base - 1));
		return tail[k][offset];
	}

	private void writeAt(int index, double value) {
		if (index < base) {
			if (head == null) {
				head = new double[base];
			}
			head[index] = value;
			return;
		}
		int adjusted = index - base;
		int unitIndex = adjusted >> log2Base;
		int k = ChunkedArrays.tailChunkIndex(unitIndex);
		int offset = ((unitIndex - (1 << k) + 1) << log2Base) + (adjusted & (base - 1));
		tail[k][offset] = value;
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

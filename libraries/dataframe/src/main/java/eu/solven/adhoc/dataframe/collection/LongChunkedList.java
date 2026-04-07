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
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.Objects;

import eu.solven.adhoc.collection.FrozenException;
import eu.solven.adhoc.collection.IFrozen;
import it.unimi.dsi.fastutil.longs.AbstractLongList;
import it.unimi.dsi.fastutil.longs.AbstractLongListIterator;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongListIterator;

/**
 * A primitive {@link LongList} backed by the same head/tail chunked layout as {@link ChunkedList}, avoiding boxing of
 * {@code long} values entirely.
 *
 * <p>
 * Storage layout (default base = 128, all arrays are {@code long[]}):
 *
 * <pre>
 * head (size 128):       indices   0 –  127
 * tail[0] (size 128):   indices 128 –  255
 * tail[1] (size 256):   indices 256 –  511
 * tail[k] (size 128·2ᵏ): …
 * </pre>
 *
 * <p>
 * See {@link ChunkedArrays} for the index-arithmetic shared with {@link ChunkedList} and {@link DoubleChunkedList}.
 *
 * @author Benoit Lacelle
 */
public class LongChunkedList extends AbstractLongList implements IFrozen {

	private final int log2Base;
	private final int base;

	// non-final: compact() may replace with a trimmed copy
	private long[] head;
	private long[][] tail;
	// PMD.AvoidFieldNameMatchingMethodName: `size` is the idiomatic AbstractList field name; renaming would deviate
	// from the JDK pattern
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	private int size;

	// private: one-way freeze transition must not be bypassed by subclasses
	private boolean compacted;

	/**
	 * Modification count for fail-fast iterator support. fastutil's {@link AbstractLongList} does not inherit
	 * {@code modCount} from {@link java.util.AbstractList}, so we declare it here.
	 */
	protected int modCount;

	/** Creates an empty list with the default base size (128). The head array is allocated lazily on first write. */
	public LongChunkedList() {
		this.log2Base = ChunkedArrays.LOG2_BASE_DEFAULT;
		this.base = ChunkedArrays.BASE_DEFAULT;
	}

	/**
	 * Creates an empty list pre-sizing the tail pointer array for up to {@code initialCapacity} elements. The base size
	 * is derived from the capacity via {@link ChunkedArrays#computeLog2Base}.
	 */
	public LongChunkedList(int initialCapacity) {
		if (initialCapacity < 0) {
			throw new IllegalArgumentException("Negative initialCapacity: " + initialCapacity);
		}
		this.log2Base = ChunkedArrays.computeLog2Base(initialCapacity);
		this.base = 1 << log2Base;
		// head allocated lazily on first write
		if (initialCapacity > base) {
			int tailSlots = ChunkedArrays.tailChunkIndex((initialCapacity - base - 1) >> log2Base) + 1;
			tail = new long[tailSlots][];
		}
	}

	@Override
	public long getLong(int index) {
		Objects.checkIndex(index, size);
		return readAt(index);
	}

	@Override
	public long set(int index, long k) {
		checkNotCompacted();
		Objects.checkIndex(index, size);
		long old = readAt(index);
		writeAt(index, k);
		return old;
	}

	@Override
	public void add(int index, long k) {
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
	public long removeLong(int index) {
		checkNotCompacted();
		Objects.checkIndex(index, size);
		long old = readAt(index);
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
		// head stays allocated if it was ever written; subsequent writes reuse it.
		size = 0;
		modCount++;
	}

	/**
	 * Returns a fail-fast list iterator starting at {@code index}. Throws {@link ConcurrentModificationException} if
	 * the list is structurally modified between iterator creation and any subsequent {@code nextLong()} /
	 * {@code previousLong()} call.
	 */
	@Override
	public LongListIterator listIterator(final int index) {
		Objects.checkIndex(index, size + 1);
		final int expectedModCount = modCount;
		return new AbstractLongListIterator() {
			private int pos = index;

			@Override
			public boolean hasNext() {
				return pos < size;
			}

			@Override
			public boolean hasPrevious() {
				return pos > 0;
			}

			@Override
			public long nextLong() {
				if (modCount != expectedModCount) {
					throw new ConcurrentModificationException();
				}
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				return readAt(pos++);
			}

			@Override
			public long previousLong() {
				if (modCount != expectedModCount) {
					throw new ConcurrentModificationException();
				}
				if (!hasPrevious()) {
					throw new NoSuchElementException();
				}
				return readAt(--pos);
			}

			@Override
			public int nextIndex() {
				return pos;
			}

			@Override
			public int previousIndex() {
				return pos - 1;
			}
		};
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
	public LongChunkedList compact() {
		if (size < base) {
			if (head != null && size < head.length) {
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
			throw new FrozenException("LongChunkedList has been compacted and is frozen");
		}
	}

	// --- internal read/write ---

	private long readAt(int index) {
		if (index < base) {
			return head[index];
		}
		int adjusted = index - base;
		int unitIndex = adjusted >> log2Base;
		int k = ChunkedArrays.tailChunkIndex(unitIndex);
		int offset = ((unitIndex - (1 << k) + 1) << log2Base) + (adjusted & (base - 1));
		return tail[k][offset];
	}

	private void writeAt(int index, long value) {
		if (index < base) {
			if (head == null) {
				head = new long[base];
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
			tail = new long[k + 1][];
		} else if (k >= tail.length) {
			tail = Arrays.copyOf(tail, k + 1);
		}
		if (tail[k] == null) {
			tail[k] = new long[base << k];
		}
	}
	// CPD-ON
}

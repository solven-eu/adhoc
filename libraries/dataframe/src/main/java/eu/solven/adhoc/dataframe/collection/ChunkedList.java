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

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.RandomAccess;

import eu.solven.adhoc.collection.FrozenException;
import eu.solven.adhoc.collection.IFrozen;

/**
 * A {@link java.util.List} backed by two separate storage areas:
 *
 * <ul>
 * <li><b>head</b> — a plain {@code Object[base]} allocated lazily on the first write. Lists with up to {@code base}
 * elements touch only this single array, with no indirection through a pointer array. The default {@code base} is
 * {@value #BASE_DEFAULT}; passing an {@code initialCapacity} larger than 128 automatically selects the largest
 * power-of-two that fits within that capacity, avoiding tail overhead for moderately sized lists.
 * <li><b>tail</b> — an {@code Object[][]} allocated on first overflow. Tail chunk {@code k} has size {@code base << k},
 * so no bulk copy is ever performed: when a new chunk is needed only that chunk's array is allocated.
 * </ul>
 *
 * <p>
 * Storage layout (default base = 128):
 *
 * <pre>
 * head (size 128):       indices   0 –  127
 * tail[0] (size 128):   indices 128 –  255
 * tail[1] (size 256):   indices 256 –  511
 * tail[2] (size 512):   indices 512 – 1023
 * tail[k] (size 128·2ᵏ): …
 * </pre>
 *
 * <p>
 * Random access is O(1). For {@code index < base} the lookup is a direct array read. For {@code index >= base}:
 *
 * <pre>
 * adjusted  = index - base
 * unitIndex = adjusted >> log2Base           // which base-sized unit
 * k         = 31 − numberOfLeadingZeros(unitIndex + 1)   // tail chunk
 * offset    = (unitIndex − (1 << k) + 1) · base + (adjusted &amp; (base − 1))
 * </pre>
 *
 * @param <E>
 *            the type of elements held in this list
 * 
 * @author Benoit Lacelle
 */
// Relates with
// https://github.com/eclipse-mat/mat/blob/master/plugins/org.eclipse.mat.report/src/org/eclipse/mat/collect/ArrayIntBig.java
public class ChunkedList<E> extends AbstractList<E> implements RandomAccess, IFrozen {

	/** Default {@code log2(base)}. Alias of {@link ChunkedArrays#LOG2_BASE_DEFAULT} kept for test access. */
	static final int LOG2_BASE_DEFAULT = ChunkedArrays.LOG2_BASE_DEFAULT;

	/** Default head size (128). Alias of {@link ChunkedArrays#BASE_DEFAULT} kept for test access. */
	static final int BASE_DEFAULT = ChunkedArrays.BASE_DEFAULT;

	/**
	 * {@code log2(base)} for this instance — drives fast bit-shift division and modulo by {@code base}. Determined at
	 * construction time from the initial capacity.
	 */
	private final int log2Base;

	/**
	 * Head size for this instance: {@code 1 << log2Base}. Lists of up to {@code base} elements use only the head array
	 * with no pointer indirection.
	 */
	private final int base;

	/**
	 * Flat array covering indices [0, base). Allocated lazily on the first write; may be shrunk by {@link #compact()}
	 * when {@code size < base}. {@code null} for a list that has never been written to.
	 */
	// non-final: lazily allocated on first write; compact() may replace it with a smaller copy
	private Object[] head;

	/**
	 * Overflow storage. {@code tail[k]} covers adjusted indices {@code [base·(2ᵏ−1), base·(2ᵏ⁺¹−1))} and has length
	 * {@code base << k}. Null until the first element beyond {@code base} is added.
	 */
	private Object[][] tail;

	// PMD.AvoidFieldNameMatchingMethodName: `size` is the idiomatic AbstractList field name; renaming would deviate
	// from the JDK pattern
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	private int size;

	/**
	 * Set to {@code true} after {@link #compact()} is called. When {@code true}, all mutating operations throw
	 * {@link UnsupportedOperationException}.
	 */
	// private: the compacted flag must not be bypassed by subclasses; compaction is a one-way transition
	private boolean compacted;

	/** Creates an empty list with the default base size (128). The head array is allocated lazily on first write. */
	public ChunkedList() {
		this.log2Base = ChunkedArrays.LOG2_BASE_DEFAULT;
		this.base = ChunkedArrays.BASE_DEFAULT;
	}

	/**
	 * Creates an empty list whose base size is derived from {@code initialCapacity}.
	 *
	 * <h3>Design rationale — why base stays at 128 for most capacities</h3>
	 *
	 * <p>
	 * The head array is allocated lazily on the first write. Choosing a large base therefore only costs memory when
	 * elements actually arrive. The tail chunks are also lazy and only allocated when the list overflows the head.
	 *
	 * <p>
	 * The base is chosen by {@link #computeLog2Base} using two thresholds relative to the next higher power of two
	 * ({@code nextPow2}):
	 * <ul>
	 * <li>If {@code capacity > 3/4 · nextPow2}: base = {@code nextPow2}. The capacity fills more than 75 % of it, so
	 * the unconditional head allocation is at most 25 % overhead. Example: {@code new ChunkedList<>(510)} → nextPow2 =
	 * 512, fill = 99 % → base = 512.
	 * <li>Otherwise: base = {@code nextPow2 / 4} (= prevPow2 / 2). Using a <em>smaller</em> base than the default
	 * reduces the unconditional head pre-allocation for capacities that are only marginally above the previous power of
	 * two. Example: {@code new ChunkedList<>(129)} → nextPow2 = 256, fill = 50 % → base = 64 (not 128).
	 * </ul>
	 *
	 * <h3>Worst-case wasted memory</h3>
	 *
	 * <p>
	 * With {@code base = 128}, the worst waste occurs just after a chunk boundary. For inputs in the range
	 * {@code (256, 512]}, three arrays are allocated: head(128) + tail[0](128) + tail[1](256) = 512 slots total. At the
	 * boundary ({@code n = 257}) only 1 slot of tail[1] is used, leaving 255 slots wasted — just under one full tail[1]
	 * chunk. In general the maximum wasted slots equal the size of the last allocated tail chunk minus one:
	 * {@code base << k − 1}.
	 *
	 * <p>
	 * The tail pointer array is also pre-sized to avoid pointer-array reallocation when growing up to
	 * {@code initialCapacity} elements.
	 */
	public ChunkedList(int initialCapacity) {
		if (initialCapacity < 0) {
			throw new IllegalArgumentException("Negative initialCapacity: " + initialCapacity);
		}
		this.log2Base = ChunkedArrays.computeLog2Base(initialCapacity);
		this.base = 1 << log2Base;
		// head allocated lazily on first write
		if (initialCapacity > base) {
			int tailSlots = ChunkedArrays.tailChunkIndex((initialCapacity - base - 1) >> log2Base) + 1;
			tail = new Object[tailSlots][];
		}
	}

	/** Creates a list containing the elements of the given collection, in iteration order. */
	@SuppressWarnings("PMD.ConstructorCallsOverridableMethod")
	public ChunkedList(Collection<? extends E> c) {
		this(c.size());
		addAll(c);
	}

	/** @see ChunkedArrays#computeLog2Base */
	static int computeLog2Base(int initialCapacity) {
		return ChunkedArrays.computeLog2Base(initialCapacity);
	}

	/** @see ChunkedArrays#tailChunkIndex */
	static int tailChunkIndex(int unitIndex) {
		return ChunkedArrays.tailChunkIndex(unitIndex);
	}

	@Override
	@SuppressWarnings("unchecked")
	public E get(int index) {
		Objects.checkIndex(index, size);
		return (E) readAt(index);
	}

	@Override
	@SuppressWarnings("unchecked")
	public E set(int index, E element) {
		checkNotCompacted();
		Objects.checkIndex(index, size);
		Object old = readAt(index);
		writeAt(index, element);
		return (E) old;
	}

	/**
	 * Appends {@code e} to the end of this list. Amortized O(1): a new tail chunk is allocated only when the previous
	 * chunk is exactly full.
	 */
	@Override
	public boolean add(E e) {
		checkNotCompacted();
		ensureTailChunkFor(size);
		writeAt(size, e);
		size++;
		modCount++;
		return true;
	}

	/** Inserts {@code element} at {@code index}, shifting subsequent elements right. O(n). */
	@Override
	public void add(int index, E element) {
		checkNotCompacted();
		Objects.checkIndex(index, size + 1);
		ensureTailChunkFor(size);
		for (int i = size; i > index; i--) {
			writeAt(i, readAt(i - 1));
		}
		writeAt(index, element);
		size++;
		modCount++;
	}

	/** Removes and returns the element at {@code index}, shifting subsequent elements left. O(n). */
	@Override
	@SuppressWarnings("unchecked")
	public E remove(int index) {
		checkNotCompacted();
		Objects.checkIndex(index, size);
		Object old = readAt(index);
		for (int i = index; i < size - 1; i++) {
			writeAt(i, readAt(i + 1));
		}
		// Null out the vacated last slot to allow GC.
		writeAt(size - 1, null);
		size--;
		modCount++;
		return (E) old;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public void clear() {
		checkNotCompacted();
		if (head != null) {
			Arrays.fill(head, 0, Math.min(size, base), null);
		}
		if (tail != null) {
			for (Object[] chunk : tail) {
				if (chunk != null) {
					Arrays.fill(chunk, null);
				}
			}
		}
		size = 0;
		modCount++;
	}

	// --- compact ---

	/**
	 * Shrinks all storage to exactly the space needed for the current elements, then marks this list as read-only.
	 *
	 * <ul>
	 * <li>If {@code size < base} the head array is replaced by a copy of length {@code size}, releasing the unused
	 * trailing slots (e.g. a 2-element list built with the default base of 128 will shrink its head from 128 slots to
	 * 2).
	 * <li>If {@code size > base} the last tail chunk is replaced by a copy trimmed to the number of slots it actually
	 * occupies.
	 * </ul>
	 *
	 * <p>
	 * After compacting, all mutating operations ({@link #add}, {@link #set}, {@link #remove}, {@link #clear}) throw
	 * {@link UnsupportedOperationException}. The operation is irreversible.
	 *
	 * @return {@code this}, for chaining
	 */
	@SuppressWarnings("PMD.LooseCoupling")
	public ChunkedList<E> compact() {
		if (size < base) {
			// Head is partially used (or still null for an empty list): shrink/trim to exact size.
			if (head != null && size < head.length) {
				head = Arrays.copyOf(head, size);
			}
		} else if (size > base && tail != null) {
			int adjusted = size - 1 - base;
			int unitIndex = adjusted >> log2Base;
			int k = ChunkedArrays.tailChunkIndex(unitIndex);
			// Compute the offset of the last occupied slot in tail[k]
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

	/** Throws {@link FrozenException} if this list has been compacted. */
	private void checkNotCompacted() {
		if (compacted) {
			throw new FrozenException("ChunkedList has been compacted and is frozen. size={}".formatted(size()));
		}
	}

	// --- internal read/write ---

	/** Reads the element at {@code index} without bounds-checking. */
	private Object readAt(int index) {
		if (index < base) {
			return head[index];
		}
		int adjusted = index - base;
		int unitIndex = adjusted >> log2Base;
		int k = ChunkedArrays.tailChunkIndex(unitIndex);
		int offset = ((unitIndex - (1 << k) + 1) << log2Base) + (adjusted & (base - 1));
		return tail[k][offset];
	}

	/** Writes {@code value} at {@code index} without bounds-checking. */
	private void writeAt(int index, Object value) {
		if (index < base) {
			if (head == null) {
				head = new Object[base];
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

	/**
	 * Ensures the tail chunk covering {@code index} exists. Does nothing for indices below {@code base} since the head
	 * is always present.
	 */
	private void ensureTailChunkFor(int index) {
		if (index < base) {
			return;
		}
		int adjusted = index - base;
		int unitIndex = adjusted >> log2Base;
		int k = ChunkedArrays.tailChunkIndex(unitIndex);
		if (tail == null) {
			tail = new Object[k + 1][];
		} else if (k >= tail.length) {
			tail = Arrays.copyOf(tail, k + 1);
		}
		if (tail[k] == null) {
			tail[k] = new Object[base << k];
		}
	}
}

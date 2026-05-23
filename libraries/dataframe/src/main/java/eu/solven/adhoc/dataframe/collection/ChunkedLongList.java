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
import it.unimi.dsi.fastutil.longs.AbstractLongList;
import it.unimi.dsi.fastutil.longs.LongList;

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
 * See {@link ChunkedArrays} for the index-arithmetic shared with {@link ChunkedList} and {@link ChunkedDoubleList}.
 *
 * <p>
 * Specialized for append-only workloads: {@link #add(long)} (and {@link #add(int, long)} when {@code index == size})
 * hits an append-cache fast path that bypasses the per-cell chunk arithmetic. Mid-list inserts and removals work but
 * invalidate the cache.
 *
 * @author Benoit Lacelle
 */
public class ChunkedLongList extends AbstractLongList implements IFreezable, ICompactable {

	private final int log2Base;
	private final int base;

	// non-final: compact() may replace with a trimmed copy
	private long @Nullable [] head;
	private long @Nullable [] @Nullable [] tail;
	// PMD.AvoidFieldNameMatchingMethodName: `size` is the idiomatic AbstractList field name; renaming would deviate
	// from the JDK pattern
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	private int size;

	// Append cache: chunk currently containing index `size` (the next append's destination), plus the global index
	// of that chunk's slot 0. `appendChunk == null` means "uninitialised / invalidated" — the next add() falls through
	// to the slow path which recomputes and repopulates the cache. The invariant is "valid for the NEXT append":
	// `set` does not touch it (size unchanged), random `add(int, long)` and `removeLong` invalidate it because the
	// chunk holding `size` may shift, `clear` resets `size` to 0 so the cache is recomputed on the next add.
	// Without this, every tail-side append paid (adjusted, unitIndex, k, offset) twice — once in
	// ensureTailChunkFor, once in writeAt — including a numberOfLeadingZeros intrinsic call.
	private long @Nullable [] appendChunk;
	private int appendChunkBase;

	// private: one-way freeze transition must not be bypassed by subclasses
	private boolean compacted;

	/** Creates an empty list with the default base size (128). The head array is allocated lazily on first write. */
	public ChunkedLongList() {
		this.log2Base = ChunkedArrays.LOG2_BASE_DEFAULT;
		this.base = ChunkedArrays.BASE_DEFAULT;
	}

	/**
	 * Creates an empty list pre-sizing the tail pointer array for up to {@code initialCapacity} elements. The base size
	 * is derived from the capacity via {@link ChunkedArrays#computeLog2Base}.
	 */
	public ChunkedLongList(int initialCapacity) {
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
		// Cache stays valid: set() does not change `size`, so the chunk holding the next-write position is unchanged.
		return old;
	}

	/**
	 * Pure append. Hot path: bypasses the bounds check and {@code index == size} branch of {@link #add(int, long)} by
	 * reading the cached {@code appendChunk} directly.
	 */
	@Override
	public boolean add(long k) {
		checkNotCompacted();
		int s = size;
		long[] wc = appendChunk;
		if (wc != null && s - appendChunkBase < wc.length) {
			wc[s - appendChunkBase] = k;
			size = s + 1;
			return true;
		}
		appendSlow(k);
		return true;
	}

	@Override
	public void add(int index, long k) {
		checkNotCompacted();
		int s = size;
		Objects.checkIndex(index, s + 1);
		if (index == s) {
			// Append shape — try the cached fast path.
			long[] wc = appendChunk;
			if (wc != null && s - appendChunkBase < wc.length) {
				wc[s - appendChunkBase] = k;
				size = s + 1;
				return;
			}
			appendSlow(k);
			return;
		}
		// Mid-list insert: the chunk for the new `size` may differ from the cached one (a shift can roll
		// the next-write position into the next chunk). Invalidating is the safest choice.
		appendChunk = null;
		ensureTailChunkFor(s);
		for (int i = s; i > index; i--) {
			writeAt(i, readAt(i - 1));
		}
		writeAt(index, k);
		size = s + 1;
	}

	// Slow path for appends: ensure the destination chunk exists, write, then repoint the append cache so
	// the next consecutive add stays on the fast path. Called when the cache is null (uninitialised, just
	// invalidated, or a chunk boundary was just crossed).
	private void appendSlow(long k) {
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
	public long removeLong(int index) {
		checkNotCompacted();
		Objects.checkIndex(index, size);
		long old = readAt(index);
		for (int i = index; i < size - 1; i++) {
			writeAt(i, readAt(i + 1));
		}
		size--;
		// Conservative invalidation: the chunk for the new `size` still points at the same chunk in practice
		// (size only shrinks), but nulling avoids one more invariant to reason about and the cost is paid on the
		// next add — the common usage pattern is append-heavy, not append-after-remove.
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
		// head stays allocated if it was ever written; subsequent writes reuse it.
		size = 0;
		// Reset the cache: the next-write position is back at index 0 (head). The slow path will
		// re-point on the first add. Nulling here keeps the recompute on a single, well-known path.
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

			long[] chunk = Objects.requireNonNull(tail[k]);
			if (offset + 1 < chunk.length) {
				tail[k] = Arrays.copyOf(chunk, offset + 1);
			}
		}
		compacted = true;
		// compact() may have replaced head or the last tail chunk with a trimmed copy; any cached reference would
		// dangle. The list is frozen against further writes anyway, but clearing keeps the invariant tidy.
		appendChunk = null;
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
			return Objects.requireNonNull(head)[index];
		}
		int adjusted = index - base;
		int unitIndex = adjusted >> log2Base;
		int k = ChunkedArrays.tailChunkIndex(unitIndex);
		int offset = ((unitIndex - (1 << k) + 1) << log2Base) + (adjusted & (base - 1));
		return Objects.requireNonNull(Objects.requireNonNull(tail)[k])[offset];
	}

	private void writeAt(int index, long value) {
		if (index < base) {
			long[] localHead = head;
			if (localHead == null) {
				localHead = new long[base];
				head = localHead;
			}
			localHead[index] = value;
			return;
		}
		int adjusted = index - base;
		int unitIndex = adjusted >> log2Base;
		int k = ChunkedArrays.tailChunkIndex(unitIndex);
		int offset = ((unitIndex - (1 << k) + 1) << log2Base) + (adjusted & (base - 1));
		long[] chunk = Objects.requireNonNull(Objects.requireNonNull(tail)[k]);
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

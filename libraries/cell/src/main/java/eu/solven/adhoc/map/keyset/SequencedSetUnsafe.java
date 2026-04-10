/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.map.keyset;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.util.cache.LastLookupCache1;

/**
 * Holds unsafe operations around {@link SequencedSetLikeList}.
 *
 * @author Benoit Lacelle
 */
public class SequencedSetUnsafe {

	// Thread-local reference-equality fast path + ConcurrentHashMap slow path.
	// Keyed solely by the caller's keys list — the delegate map is shared across all callers.
	private static final LastLookupCache1<SequencedSetLikeList> LIST_TO_KEYSET_CACHE = new LastLookupCache1<>();

	protected SequencedSetUnsafe() {
		// Utility class
	}

	/**
	 * Interns the given keys into a {@link SequencedSetLikeList}, caching the result by reference identity of
	 * {@code keys}.
	 *
	 * <p>
	 * Fast-path hits rely on {@code keys} being passed as the same reference as a previous call on the current thread.
	 * When {@code keys} is a Guava {@link ImmutableList}, {@link ImmutableList#copyOf} short-circuits and returns the
	 * same reference, enabling reference-equality hits across calls that reuse the same list.
	 *
	 * @param keys
	 *            the keys to intern
	 * @return an interned {@link SequencedSetLikeList} for the given keys
	 */
	public static SequencedSetLikeList internKeyset(Collection<? extends String> keys) {
		List<String> keysAsList = ImmutableList.copyOf(keys);

		// Fast path: reference equality on keysAsList
		SequencedSetLikeList cached = LIST_TO_KEYSET_CACHE.getByRef(keysAsList);
		if (cached != null) {
			return cached;
		}

		// Slow path
		return LIST_TO_KEYSET_CACHE.slowComputeIfAbsent(keysAsList,
				() -> SequencedSetLikeList.fromCollection(keysAsList));
	}

	/**
	 * Clears the static keyset-interning cache. Thread-local last-entries will miss on next access and be refreshed.
	 */
	public static void invalidateAll() {
		LIST_TO_KEYSET_CACHE.clear();
	}
}

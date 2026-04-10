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
package eu.solven.adhoc.util.cache;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * A two-layer cache: a thread-local last-entry fast path checked by <strong>reference equality</strong> on N key parts,
 * backed by a {@link ConcurrentHashMap} slow path keyed by the same parts wrapped in a {@link List}.
 *
 * <p>
 * Usage pattern: call {@link #getByRef} first (zero allocation). On miss ({@code null} return), call
 * {@link #slowComputeIfAbsent} which wraps the key parts into a {@link List} for the delegate map, computes the value
 * if absent, and updates the thread-local entry. This two-step API avoids lambda allocation on the fast path.
 *
 * <p>
 * The key length (number of parts) is fixed at construction time so that the fast-path reference check can skip the
 * length comparison and assume both arrays are of {@link #getKeyLength()}.
 *
 * @param <V>
 *            the value type
 * @author Benoit Lacelle
 */
public class LastLookupCache<V> {

	private final int keyLength;
	private final ConcurrentMap<List<Object>, V> delegate;
	private final ThreadLocal<CacheEntry<V>> lastEntry;

	/**
	 * Mutable holder for the thread-local last-entry. Avoids allocating new objects on each cache update.
	 */
	protected static class CacheEntry<V> {
		/** The reference-key parts from the last successful lookup. */
		protected Object[] refKeys;

		/** The cached value from the last successful lookup. */
		protected V value;
	}

	/**
	 * Creates a cache backed by the given map.
	 *
	 * @param keyLength
	 *            the fixed number of key parts (must be &gt; 0)
	 * @param delegate
	 *            the backing slow-path map
	 */
	public LastLookupCache(int keyLength, ConcurrentMap<List<Object>, V> delegate) {
		if (keyLength <= 0) {
			throw new IllegalArgumentException("keyLength must be > 0, got " + keyLength);
		}
		this.keyLength = keyLength;
		this.delegate = delegate;
		this.lastEntry = ThreadLocal.withInitial(() -> {
			CacheEntry<V> entry = new CacheEntry<>();
			// Sentinel array of the right length, filled with null — guarantees the first call misses.
			entry.refKeys = new Object[keyLength];
			return entry;
		});
	}

	/**
	 * Creates a cache backed by a new {@link ConcurrentHashMap}.
	 *
	 * @param keyLength
	 *            the fixed number of key parts (must be &gt; 0)
	 */
	public LastLookupCache(int keyLength) {
		this(keyLength, new ConcurrentHashMap<>());
	}

	/**
	 * @return the fixed number of key parts expected by {@link #getByRef} and {@link #slowComputeIfAbsent}.
	 */
	public int getKeyLength() {
		return keyLength;
	}

	/**
	 * Fast path: returns the cached value if all {@code refKeys} match the last lookup on the current thread by
	 * reference equality ({@code ==}). Returns {@code null} on miss — caller should then call
	 * {@link #slowComputeIfAbsent}.
	 *
	 * <p>
	 * Zero allocation: no key object built, no hashCode computed, no lambda captured.
	 *
	 * @param refKeys
	 *            the individual key parts for reference equality check — must be of length {@link #getKeyLength()}
	 * @return the cached value, or {@code null} on miss
	 */
	public V getByRef(Object... refKeys) {
		assert refKeys.length == keyLength : "Expected " + keyLength + " key parts, got " + refKeys.length;

		CacheEntry<V> entry = lastEntry.get();

		if (referenceKeysMatch(entry.refKeys, refKeys)) {
			return entry.value;
		}

		return null;
	}

	/**
	 * Slow path: wraps {@code refKeys} in a {@link List} as the composite map key, computes the value via
	 * {@code valueSupplier} if absent, and updates the thread-local entry for future fast-path hits.
	 *
	 * @param refKeys
	 *            the same key parts passed to {@link #getByRef} — stored for future reference-equality checks and
	 *            wrapped as a {@link List} for the map key. Must be of length {@link #getKeyLength()}.
	 * @param valueSupplier
	 *            computes the value when absent from the delegate map
	 * @return the computed or existing value
	 */
	public V slowComputeIfAbsent(Object[] refKeys, Supplier<V> valueSupplier) {
		assert refKeys.length == keyLength : "Expected " + keyLength + " key parts, got " + refKeys.length;

		List<Object> key = Arrays.asList(refKeys);
		V value = delegate.computeIfAbsent(key, k -> valueSupplier.get());

		// Update thread-local cache for future fast-path hits
		CacheEntry<V> entry = lastEntry.get();
		entry.refKeys = refKeys;
		entry.value = value;

		return value;
	}

	/**
	 * Checks reference equality ({@code ==}) on all corresponding key parts. Both arrays are guaranteed by construction
	 * to have the same length, so no length check is needed.
	 */
	// Reference equality is intentional (the whole point of the fast-path cache).
	@SuppressWarnings({ "PMD.UseVarargs", "PMD.CompareObjectsWithEquals" })
	protected boolean referenceKeysMatch(Object[] cached, Object[] current) {
		// Start from last index as keys are generally built from general to specific
		for (int i = keyLength - 1; i >= 0; i--) {
			if (cached[i] != current[i]) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Clears the backing map. Thread-local entries will miss on next access and be refreshed.
	 */
	public void clear() {
		delegate.clear();
	}
}

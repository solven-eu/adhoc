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
package eu.solven.adhoc.util.map;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A two-layer cache: a thread-local last-entry fast path checked by <strong>reference equality</strong> on N key parts,
 * backed by a {@link ConcurrentHashMap} slow path.
 *
 * <p>
 * Usage pattern: call {@link #getByRef} first (zero allocation). On miss ({@code null} return), call
 * {@link #slowComputeIfAbsent} which builds the map key, computes the value if absent, and updates the thread-local
 * entry. This two-step API avoids lambda allocation on the fast path.
 *
 * @param <K>
 *            the delegate map key type
 * @param <V>
 *            the value type
 * @author Benoit Lacelle
 */
public class LastLookupCache<K, V> {

	private final Map<K, V> delegate;

	/**
	 * Mutable holder for the thread-local last-entry. Avoids allocating new objects on each cache update.
	 */
	protected static class CacheEntry<V> {
		/** The reference-key parts from the last successful lookup. */
		protected Object[] refKeys = {};

		/** The cached value from the last successful lookup. */
		protected V value;
	}

	private final ThreadLocal<CacheEntry<V>> lastEntry = ThreadLocal.withInitial(CacheEntry::new);

	/**
	 * Creates a cache backed by the given map.
	 */
	public LastLookupCache(Map<K, V> delegate) {
		this.delegate = delegate;
	}

	/**
	 * Creates a cache backed by a new {@link ConcurrentHashMap}.
	 */
	public LastLookupCache() {
		this(new ConcurrentHashMap<>());
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
	 *            the individual key parts for reference equality check
	 * @return the cached value, or {@code null} on miss
	 */
	public V getByRef(Object... refKeys) {
		CacheEntry<V> entry = lastEntry.get();

		if (referenceKeysMatch(entry.refKeys, refKeys)) {
			return entry.value;
		}

		return null;
	}

	/**
	 * Slow path: builds the map key via {@code keySupplier}, computes the value via {@code mappingFunction} if absent,
	 * and updates the thread-local entry for future fast-path hits.
	 *
	 * @param refKeys
	 *            the same key parts passed to {@link #getByRef} — stored for future reference-equality checks
	 * @param keySupplier
	 *            builds the composite map key
	 * @param mappingFunction
	 *            computes absent values
	 * @return the computed or existing value
	 */
	public V slowComputeIfAbsent(Object[] refKeys, Supplier<K> keySupplier, Function<K, V> mappingFunction) {
		K key = keySupplier.get();
		V value = delegate.computeIfAbsent(key, mappingFunction);

		// Update thread-local cache for future fast-path hits
		CacheEntry<V> entry = lastEntry.get();
		entry.refKeys = refKeys;
		entry.value = value;

		return value;
	}

	/**
	 * Checks reference equality ({@code ==}) on all corresponding key parts.
	 */
	protected static boolean referenceKeysMatch(Object[] cached, Object[] current) {
		if (cached.length != current.length) {
			return false;
		}
		for (int i = 0; i < cached.length; i++) {
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

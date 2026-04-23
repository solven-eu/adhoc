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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * A two-layer cache specialized for a single-part key. A thread-local last-entry fast path checked by <strong>reference
 * equality</strong> ({@code ==}), backed by a {@link ConcurrentHashMap} slow path keyed by the key directly (no
 * {@link java.util.List} wrapping).
 *
 * <p>
 * This is the length-1 specialization of {@link LastLookupCache}: it avoids the {@code Object[]} varargs allocation and
 * the list wrapping on the slow path, and removes the per-element loop from the reference check. Use on hot paths where
 * a single reference key is sufficient to disambiguate cached values.
 *
 * <p>
 * Null keys are not supported: the backing {@link ConcurrentHashMap} rejects them, and the initial thread-local state
 * stores a {@code null} sentinel so the first {@link #getByRef} call always misses.
 *
 * @param <V>
 *            the value type
 * @author Benoit Lacelle
 */
public class LastLookupCache1<V> {

	/** Sentinel initial entry — {@code null} key guarantees the first {@link #getByRef} call misses. */
	private static final CacheEntry<Object, Object> EMPTY = new CacheEntry<>(null, null);

	private final ConcurrentMap<Object, V> delegate;
	private final ThreadLocal<CacheEntry<Object, V>> lastEntry = ThreadLocal.withInitial(LastLookupCache1::emptyEntry);

	@SuppressWarnings("unchecked")
	private static <V> CacheEntry<Object, V> emptyEntry() {
		return (CacheEntry<Object, V>) EMPTY;
	}

	/**
	 * Creates a cache backed by the given map.
	 */
	public LastLookupCache1(ConcurrentMap<Object, V> delegate) {
		this.delegate = delegate;
	}

	/**
	 * Creates a cache backed by a new {@link ConcurrentHashMap}.
	 */
	public LastLookupCache1() {
		this(new ConcurrentHashMap<>());
	}

	/**
	 * Fast path: returns the cached value if {@code refKey} is reference-equal ({@code ==}) to the last lookup on the
	 * current thread. Returns {@code null} on miss — caller should then call {@link #slowComputeIfAbsent}.
	 *
	 * <p>
	 * Zero allocation: no hashCode computed, no lambda captured.
	 *
	 * @param refKey
	 *            the key for reference-equality check — must be non-null
	 * @return the cached value, or {@code null} on miss
	 */
	// Reference equality is intentional (the whole point of the fast-path cache).
	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	public V getByRef(Object refKey) {
		CacheEntry<Object, V> entry = lastEntry.get();

		if (entry.key == refKey) {
			return entry.value;
		}

		return null;
	}

	/**
	 * Slow path: uses {@code refKey} as the composite map key, computes the value via {@code valueSupplier} if absent,
	 * and updates the thread-local entry for future fast-path hits.
	 *
	 * @param refKey
	 *            the same key passed to {@link #getByRef} — stored for future reference-equality checks and used as the
	 *            delegate map key. Must be non-null.
	 * @param valueSupplier
	 *            computes the value when absent from the delegate map
	 * @return the computed or existing value
	 */
	public V slowComputeIfAbsent(Object refKey, Supplier<V> valueSupplier) {
		V value = delegate.computeIfAbsent(refKey, k -> valueSupplier.get());

		// Publish a fresh immutable entry for future fast-path hits
		lastEntry.set(new CacheEntry<>(refKey, value));

		return value;
	}

	/**
	 * Clears the backing map. Thread-local entries will miss on next access and be refreshed.
	 */
	public void clear() {
		delegate.clear();
	}
}

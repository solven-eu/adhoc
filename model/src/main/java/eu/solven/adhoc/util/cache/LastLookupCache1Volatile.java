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
 * Drop-in variant of {@link LastLookupCache1} that replaces the {@link ThreadLocal} last-entry holder with a single
 * {@code volatile} reference shared by all threads.
 *
 * <p>
 * Pros vs. {@link LastLookupCache1}: no {@link ThreadLocal#get()} map lookup on the fast path — reading a volatile is a
 * single memory load.
 *
 * <p>
 * Cons: a single shared slot means threads compete for the cache. Under concurrent access with diverging key sequences,
 * threads constantly overwrite each other's last-entry, collapsing the hit rate. This variant is therefore best suited
 * to workloads where one thread dominates the hot loop (typical single-threaded query execution), or where all threads
 * repeatedly hit the same small set of keys.
 *
 * <p>
 * Races are tolerated: the worst case is an unnecessary slow-path call. Correctness relies only on the ref-equality
 * check and the delegate {@link ConcurrentHashMap}.
 *
 * @param <V>
 *            the value type
 * @author Benoit Lacelle
 */
public class LastLookupCache1Volatile<V> {

	private final ConcurrentMap<Object, V> delegate;

	/**
	 * Shared, racy last-entry. Both fields are read/written together via {@link #lastEntry} so we only need one
	 * volatile hop.
	 */
	// Intentional: a single shared volatile slot is the whole point of this variant.
	@SuppressWarnings("PMD.AvoidUsingVolatile")
	private volatile CacheEntry<V> lastEntry = new CacheEntry<>();

	/**
	 * Immutable holder so that a racing reader never sees a half-published entry (refKey from a new put, value from the
	 * previous put). Allocated on each slow-path update — cheap given the slow path already builds more objects.
	 */
	protected static final class CacheEntry<V> {
		private final Object refKey;
		private final V value;

		protected CacheEntry() {
			this.refKey = null;
			this.value = null;
		}

		protected CacheEntry(Object refKey, V value) {
			this.refKey = refKey;
			this.value = value;
		}
	}

	/**
	 * Creates a cache backed by the given map.
	 */
	public LastLookupCache1Volatile(ConcurrentMap<Object, V> delegate) {
		this.delegate = delegate;
	}

	/**
	 * Creates a cache backed by a new {@link ConcurrentHashMap}.
	 */
	public LastLookupCache1Volatile() {
		this(new ConcurrentHashMap<>());
	}

	/**
	 * Fast path: returns the cached value if {@code refKey} is reference-equal ({@code ==}) to the last lookup on any
	 * thread. Returns {@code null} on miss — caller should then call {@link #slowComputeIfAbsent}.
	 *
	 * @param refKey
	 *            the key for reference-equality check — must be non-null
	 * @return the cached value, or {@code null} on miss
	 */
	// Reference equality is intentional (the whole point of the fast-path cache).
	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	public V getByRef(Object refKey) {
		CacheEntry<V> entry = lastEntry;
		if (entry.refKey == refKey) {
			return entry.value;
		}
		return null;
	}

	/**
	 * Slow path: uses {@code refKey} as the delegate map key, computes the value via {@code valueSupplier} if absent,
	 * and publishes a fresh last-entry for future fast-path hits on any thread.
	 *
	 * @param refKey
	 *            the same key passed to {@link #getByRef} — must be non-null
	 * @param valueSupplier
	 *            computes the value when absent from the delegate map
	 * @return the computed or existing value
	 */
	public V slowComputeIfAbsent(Object refKey, Supplier<V> valueSupplier) {
		V value = delegate.computeIfAbsent(refKey, k -> valueSupplier.get());

		// Publish atomically via a fresh immutable holder.
		lastEntry = new CacheEntry<>(refKey, value);

		return value;
	}

	/**
	 * Clears the backing map and resets the shared last-entry.
	 */
	public void clear() {
		delegate.clear();
		lastEntry = new CacheEntry<>();
	}
}

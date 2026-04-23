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

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * JDK-25 {@link ScopedValue}-backed variant of {@link LastLookupCache1}: the "last entry" is held in a tiny mutable
 * holder bound per scope instead of a {@link ThreadLocal}.
 *
 * Fast-path semantics are identical to {@link LastLookupCache1} — reference equality on {@code refKey}. The difference
 * is entirely about the fast-path cost: a bound {@link ScopedValue#get()} is cheaper than {@link ThreadLocal#get()} on
 * JDK 25 (especially under Virtual Threads), at the price of requiring callers to establish a scope via
 * {@link #runInScope(Runnable)} or {@link #callInScope(Callable)}.
 *
 * <b>Out-of-scope behaviour</b> — unlike {@code ScopedValueAppendableTable}, this cache silently no-ops when no scope
 * is bound: {@link #getByRef} returns {@code null} (permanent miss) and {@link #slowComputeIfAbsent} skips publishing
 * to the scope's holder. This keeps the cache callable from static contexts without breaking them; the only cost is
 * zero hit rate outside a scope, which is equivalent to a cold cache.
 *
 * <b>Mutable holder, per scope</b> — {@link ScopedValue} bindings are immutable, so we bind a small single-field holder
 * object at scope entry and mutate its fields inside the scope. This is single-threaded by construction (a scope is
 * tied to one thread's dynamic extent), so no synchronisation is needed.
 *
 * @param <V>
 *            the value type
 * @author Benoit Lacelle
 */
public class LastLookupCache1Scoped<V> {

	private final ScopedValue<Holder> scope = ScopedValue.newInstance();
	private final ConcurrentMap<Object, V> delegate;

	// Mutable 1-cell holder. A scope binds one fresh instance; mutations happen inside the scope on a single thread.
	// Not an AtomicReference, not volatile: no cross-thread visibility concerns inside a per-thread scope.
	private static final class Holder {
		Object key;
		Object value;
	}

	/**
	 * Creates a cache backed by a fresh {@link ConcurrentHashMap}.
	 */
	public LastLookupCache1Scoped() {
		this(new ConcurrentHashMap<>());
	}

	/**
	 * Creates a cache backed by the given map.
	 */
	public LastLookupCache1Scoped(ConcurrentMap<Object, V> delegate) {
		this.delegate = delegate;
	}

	/**
	 * Fast path: returns the cached value if {@code refKey} is reference-equal ({@code ==}) to the last lookup in the
	 * current scope. Returns {@code null} on miss — including when no scope is bound.
	 *
	 * @param refKey
	 *            the key for reference-equality check — must be non-null
	 * @return the cached value, or {@code null} on miss
	 */
	// Reference equality is intentional (the whole point of the fast-path cache).
	@SuppressWarnings({ "unchecked", "PMD.CompareObjectsWithEquals" })
	public V getByRef(Object refKey) {
		// `ScopedValue.orElse(null)` throws NPE in JDK 25 (Objects.requireNonNull on the fallback), so we have to use
		// the isBound() + get() pair even though it's two probes.
		if (!scope.isBound()) {
			return null;
		}
		Holder h = scope.get();
		if (h.key == refKey) {
			return (V) h.value;
		}
		return null;
	}

	/**
	 * Slow path: uses {@code refKey} as the delegate map key, computes the value via {@code valueSupplier} if absent,
	 * and updates the current scope's holder for future fast-path hits.
	 *
	 * @param refKey
	 *            the same key passed to {@link #getByRef} — used as the delegate map key. Must be non-null.
	 * @param valueSupplier
	 *            computes the value when absent from the delegate map
	 * @return the computed or existing value
	 */
	public V slowComputeIfAbsent(Object refKey, Supplier<V> valueSupplier) {
		V value = delegate.computeIfAbsent(refKey, k -> valueSupplier.get());
		// `ScopedValue.orElse(null)` throws NPE in JDK 25, so probe with isBound() before publishing.
		if (scope.isBound()) {
			Holder h = scope.get();
			h.key = refKey;
			h.value = value;
		}
		return value;
	}

	/**
	 * Runs {@code body} with a fresh per-scope holder bound, enabling fast-path hits inside {@code body}.
	 */
	public void runInScope(Runnable body) {
		ScopedValue.where(scope, new Holder()).run(body);
	}

	/**
	 * {@link #runInScope(Runnable)} for a {@link Callable} body. Propagates checked exceptions as-is.
	 *
	 * @return the value returned by {@code body}.
	 */
	// Mirrors Callable.call's `throws Exception` to propagate the body's checked exceptions transparently.
	@SuppressWarnings("PMD.SignatureDeclareThrowsException")
	public <R> R callInScope(Callable<R> body) throws Exception {
		// Callable.call() matches the signature of ScopedValue.CallableOp<R, Exception>.call().
		return ScopedValue.where(scope, new Holder()).call(body::call);
	}

	/**
	 * Clears the backing map. A currently-active scope's holder still contains its last entry, but further fast-path
	 * hits only apply when the exact same reference key is queried — so stale entries surface at most once per active
	 * scope, and consistently re-populate via the slow path afterwards.
	 */
	public void clear() {
		delegate.clear();
	}
}

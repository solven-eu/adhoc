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
package eu.solven.adhoc.encoding.page;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

import com.google.errorprone.annotations.ThreadSafe;

import eu.solven.adhoc.encoding.perfect_hashing.MutablePerfectHashMap;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link IAppendableTable} backed by a {@link ScopedValue} — the scoped analogue of {@link ThreadLocalAppendableTable}.
 * Requires JDK 25+ (JEP 506 finalized {@code ScopedValue}).
 *
 * Benchmarks suggest {@code ScopedValue.get()} is cheaper than {@code ThreadLocal.get()} on typical JDK 25 setups
 * (especially under Virtual Threads), because a scoped binding is immutable for its lifetime and avoids the hash lookup
 * in {@code Thread.threadLocals}.
 *
 * <b>Usage contract</b> — each caller thread MUST establish its own scope before invoking {@link #nextRow}, e.g. via
 * {@link #runInScope(Runnable)} or {@link #callInScope(Callable)}. Calling {@link #nextRow} outside a scope throws
 * {@link NoSuchElementException} (propagated from {@link ScopedValue#get()}).
 *
 * <b>Thread-safety</b> — a single instance is safe to use from multiple threads as long as each thread binds its own
 * scope, giving each thread its own page map. This class is NOT designed for threads sharing a single scope (e.g.
 * children forked from a {@code StructuredTaskScope}): those would race on the same mutable map, breaking the
 * mono-threaded per-page writes assumption inherited from {@link ThreadLocalAppendableTable}.
 *
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
@ThreadSafe
public class ScopedValueAppendableTable extends AAppendableTable {

	/**
	 * Holds the per-scope page map. Exposed as package-private so tests can introspect binding state via
	 * {@link ScopedValue#isBound()} without going through {@link #nextRow}.
	 */
	final ScopedValue<Map<List<String>, IAppendableTablePage>> keyToPage = ScopedValue.newInstance();

	@Override
	protected IAppendableTablePage getCurrentPage(List<String> keysAsList) {
		return keyToPage.get().computeIfAbsent(keysAsList, _ -> makePage());
	}

	@Override
	protected boolean compareAndSetPage(List<String> keysAsList,
			IAppendableTablePage currentPage,
			IAppendableTablePage newCandidate) {
		return keyToPage.get().replace(keysAsList, currentPage, newCandidate);
	}

	/**
	 * Runs {@code body} with a fresh page map bound to this table's scoped value. Pages created inside the scope are
	 * released once it exits; the binding is visible to structured-concurrency children spawned inside {@code body}.
	 */
	public void runInScope(Runnable body) {
		ScopedValue.where(keyToPage, new MutablePerfectHashMap<>()).run(body);
	}

	/**
	 * {@link #runInScope(Runnable)} for a {@link Callable} body. Propagates checked exceptions as-is.
	 *
	 * @return the value returned by {@code body}.
	 */
	// Mirrors Callable.call's `throws Exception` to propagate the body's checked exceptions transparently.
	@SuppressWarnings("PMD.SignatureDeclareThrowsException")
	public <R> R callInScope(Callable<R> body) throws Exception {
		// Callable.call() has the same signature as ScopedValue.CallableOp<R, Exception>.call(), so a method
		// reference adapts between the two SAM interfaces without an intermediate allocation.
		return ScopedValue.where(keyToPage, new MutablePerfectHashMap<>()).call(body::call);
	}
}

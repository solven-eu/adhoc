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
package eu.solven.adhoc.engine.options;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.options.IQueryOption;
import lombok.experimental.UtilityClass;

/**
 * Thread-scoped access to the executing query's options ({@code Set<IQueryOption>}). Sibling to
 * {@link CustomMarkerScope} — same mechanism, same module, same binding site ({@code CubeQueryEngine#execute}).
 *
 * <p>
 * Use case: a complex {@code ICalculatedColumn} or {@code ICalculatedCoordinate} that performs side effects (database
 * round-trip, expensive computation, external cache lookup) often needs to know whether the caller asked for
 * {@code StandardQueryOptions.EXPLAIN} (be verbose, log SQL) or {@code StandardQueryOptions.NO_CACHE} (skip the
 * external cache lookup). Without this scope, the extension point would have to encode the options into its definition
 * or rely on a side-channel; with it, it just calls {@link #isActive(IQueryOption)}.
 *
 * <p>
 * Backed by {@link ScopedValue} (JEP 506, finalised in JDK 25). Composition rules mirror {@link CustomMarkerScope}:
 * each {@code CubeQueryEngine#execute} binds its query's options, composite sub-cubes re-enter and rebind, and
 * {@link #current()} returns {@link Set#of()} (empty set) when no scope is active so callers do not have to null-check.
 *
 * @author Benoit Lacelle
 */
@UtilityClass
public final class QueryOptionsScope {

	// Empty set used both as the bound value when callers pass null AND as the read-side default outside any scope.
	// Keeping these two cases collapsed simplifies the reader contract: "no scope" and "scope with no options" are
	// operationally equivalent — neither activates any option.
	private static final Set<IQueryOption> EMPTY = ImmutableSet.of();

	private static final ScopedValue<Set<IQueryOption>> CURRENT_OPTIONS = ScopedValue.newInstance();

	/**
	 * Binds {@code options} as the current query's option set for the duration of {@code body} and returns the body's
	 * value. {@code null} is normalised to an empty set so callers do not have to guard against an unset query.
	 *
	 * @param <R>
	 *            the body's return type
	 * @param options
	 *            the option set to expose; {@code null} is treated as no options
	 * @param body
	 *            the body to run inside the scope
	 * @return the value returned by {@code body}
	 * @throws Exception
	 *             any checked exception propagated from {@code body}
	 */
	// Mirrors Callable.call's `throws Exception` to propagate the body's checked exceptions transparently.
	@SuppressWarnings("PMD.SignatureDeclareThrowsException")
	public static <R> R callWith(Set<IQueryOption> options, Callable<R> body) throws Exception {
		Set<IQueryOption> bound;
		if (options == null) {
			bound = EMPTY;
		} else {
			bound = ImmutableSet.copyOf(options);
		}
		return ScopedValue.where(CURRENT_OPTIONS, bound).call(body::call);
	}

	/**
	 * {@link #callWith(Set, Callable)} variant for non-throwing bodies — wraps checked exceptions in
	 * {@link IllegalStateException}.
	 *
	 * @param <R>
	 *            the body's return type
	 * @param options
	 *            the option set to expose; {@code null} is treated as no options
	 * @param body
	 *            the body to run inside the scope
	 * @return the value returned by {@code body}
	 */
	public static <R> R runWith(Set<IQueryOption> options, Supplier<R> body) {
		try {
			return callWith(options, body::get);
		} catch (RuntimeException | Error e) {
			throw e;
		} catch (Exception e) {
			// Unreachable for a Supplier body — protects against custom CallableOp overrides.
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Returns the current query's options, or an empty set if no scope is active.
	 *
	 * @return the bound option set; never null
	 */
	public static Set<IQueryOption> current() {
		if (CURRENT_OPTIONS.isBound()) {
			return CURRENT_OPTIONS.get();
		}
		return EMPTY;
	}

	/**
	 * Convenience: returns {@code true} if {@code option} is in the currently-bound option set. Equivalent to
	 * {@code current().contains(option)} but reads more naturally at call sites — e.g.
	 * {@code QueryOptionsScope.isActive(StandardQueryOptions.NO_CACHE)}.
	 *
	 * @param option
	 *            the option to test
	 * @return {@code true} if the option is currently active
	 */
	public static boolean isActive(IQueryOption option) {
		return option.isActive(current());
	}

	/**
	 * Returns {@code true} if a scope is currently bound on this thread. Useful to distinguish "no scope" from "scope
	 * with empty options" for diagnostics — both return an empty set from {@link #current()}.
	 *
	 * @return {@code true} if a scope is active
	 */
	public static boolean isBound() {
		return CURRENT_OPTIONS.isBound();
	}

}

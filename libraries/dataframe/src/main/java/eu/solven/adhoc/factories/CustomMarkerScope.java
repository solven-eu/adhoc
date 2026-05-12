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
package eu.solven.adhoc.factories;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;

/**
 * Thread-scoped access to the executing query's {@code customMarker}, without threading it through every internal API.
 *
 * <p>
 * Background: extension points that take no contextual argument (e.g.
 * {@code ICalculatedColumn#computeCoordinate(record)}, an {@code ICalculatedCoordinate}'s filter evaluator)
 * historically had no way to vary their behaviour based on the query's {@code customMarker}. The choice was either to
 * (a) widen every signature on the path between the engine and the extension point, or (b) encode the marker into the
 * column / coord definition itself. Both are intrusive. This class is the third option: the engine binds the current
 * query's {@code customMarker} to a thread-scoped value for the duration of execution, and the extension point reads it
 * via {@link #current()} when it needs to. Code that does not care reads nothing and pays nothing.
 *
 * <p>
 * Backed by {@link ScopedValue} (JEP 506, finalised in JDK 25). The same module already uses {@code ScopedValue}
 * elsewhere ({@code ScopedValueAppendableTable}), so this raises no new platform requirement.
 *
 * <p>
 * Composition rules:
 * <ul>
 * <li>Each {@code CubeQueryEngine#execute} binds its query's {@code customMarker}. Composite-cube fan-out re-enters
 * {@code execute} per sub-cube, so each sub-query naturally rebinds with its own (possibly transcoded) marker.</li>
 * <li>{@link #current()} returns {@link Optional#empty()} both when no scope is active (e.g. unit tests not driven
 * through the engine) and when the active query's marker is {@code null}. Callers that need to distinguish those two
 * cases should not — they are operationally equivalent ("no marker to act on").</li>
 * </ul>
 *
 * @author Benoit Lacelle
 */
public final class CustomMarkerScope {

	// Wrapped in Optional so a null customMarker is representable: ScopedValue#where rejects null values on some JDK
	// builds, and we want a single uniform read-path. Optional.empty() also signals "no active scope" — see #current.
	private static final ScopedValue<Optional<Object>> CURRENT_MARKER = ScopedValue.newInstance();

	private CustomMarkerScope() {
		// Utility class with static helpers only.
	}

	/**
	 * Binds {@code customMarker} as the current query's marker for the duration of {@code body} and returns the body's
	 * value. Propagates checked exceptions from the body as-is.
	 *
	 * @param <R>
	 *            the body's return type
	 * @param customMarker
	 *            the marker to expose to readers via {@link #current()}; may be {@code null}
	 * @param body
	 *            the body to run inside the scope
	 * @return the value returned by {@code body}
	 * @throws Exception
	 *             any checked exception propagated from {@code body}
	 */
	// Mirrors Callable.call's `throws Exception` to propagate the body's checked exceptions transparently.
	@SuppressWarnings("PMD.SignatureDeclareThrowsException")
	public static <R> R callWith(@Nullable Object customMarker, Callable<R> body) throws Exception {
		return ScopedValue.where(CURRENT_MARKER, Optional.ofNullable(customMarker)).call(body::call);
	}

	/**
	 * {@link #callWith(Object, Callable)} variant for non-throwing bodies — wraps checked exceptions in
	 * {@link IllegalStateException}.
	 *
	 * @param <R>
	 *            the body's return type
	 * @param customMarker
	 *            the marker to expose to readers via {@link #current()}; may be {@code null}
	 * @param body
	 *            the body to run inside the scope
	 * @return the value returned by {@code body}
	 */
	public static <R> R runWith(@Nullable Object customMarker, Supplier<R> body) {
		try {
			return callWith(customMarker, body::get);
		} catch (RuntimeException | Error e) {
			throw e;
		} catch (Exception e) {
			// Unreachable for a Supplier body — protects against custom CallableOp overrides that surface checked
			// exceptions.
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Returns the current query's {@code customMarker}, or {@link Optional#empty()} if no scope is active or the active
	 * scope's marker is {@code null}.
	 *
	 * @return the bound marker, or empty
	 */
	public static Optional<Object> current() {
		if (CURRENT_MARKER.isBound()) {
			return CURRENT_MARKER.get();
		}
		return Optional.empty();
	}

	/**
	 * Returns {@code true} if a query scope is currently bound on this thread. Useful to distinguish "no scope" from
	 * "in a scope with a null marker" when that distinction matters (e.g. diagnostics).
	 *
	 * @return {@code true} if a scope is active
	 */
	public static boolean isBound() {
		return CURRENT_MARKER.isBound();
	}

}

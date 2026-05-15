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
package eu.solven.adhoc.query;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;

/**
 * Thread-scoped override for the engine-side {@link AdhocQueryId#getQueryId()} UUID. When a Pivotable / API frontend
 * pre-generates a tracking UUID and wants the engine to adopt it (so the same UUID can be reused across the HTTP
 * surface, the Live View, and the registry), it binds the value here for the duration of the submission call. The
 * preparator (see {@link AdhocQueryIds#from(String, Object)}) reads the binding inside {@code prepareQuery(...)} —
 * which runs synchronously on the submitting thread, before any async future is scheduled — and threads it into the
 * {@link AdhocQueryId} builder.
 *
 * <p>
 * Without a binding, {@link AdhocQueryIds#from} falls back to {@code AdhocQueryId.queryId}'s default random UUID, so
 * existing callers see no change. Mirrors the shape of {@code CustomMarkerScope} / {@code QueryOptionsScope} so the
 * three thread-scoped engine inputs have a uniform API.
 *
 * @author Benoit Lacelle
 */
public final class SubmittedQueryIdScope {

	// Wrapped in Optional so a null queryId is representable and the read-path is uniform with the other scopes.
	private static final ScopedValue<Optional<UUID>> CURRENT_QUERY_ID = ScopedValue.newInstance();

	private SubmittedQueryIdScope() {
		// Utility class with static helpers only.
	}

	/**
	 * Binds {@code queryId} for the duration of {@code body}.
	 *
	 * @param <R>
	 *            the body's return type
	 * @param queryId
	 *            the UUID the engine should adopt as {@link AdhocQueryId#getQueryId()}; may be {@code null} (acts as a
	 *            no-op binding — equivalent to no scope at all from the reader's perspective)
	 * @param body
	 *            the body to run inside the scope
	 * @return the value returned by {@code body}
	 * @throws Exception
	 *             any checked exception propagated from {@code body}
	 */
	// Mirrors Callable.call's `throws Exception` to propagate the body's checked exceptions transparently.
	@SuppressWarnings("PMD.SignatureDeclareThrowsException")
	public static <R> R callWith(@Nullable UUID queryId, Callable<R> body) throws Exception {
		return ScopedValue.where(CURRENT_QUERY_ID, Optional.ofNullable(queryId)).call(body::call);
	}

	/**
	 * Non-throwing variant of {@link #callWith(UUID, Callable)} — wraps checked exceptions as
	 * {@link IllegalStateException}.
	 *
	 * @param <R>
	 *            the body's return type
	 * @param queryId
	 *            the UUID; may be {@code null}
	 * @param body
	 *            the body to run inside the scope
	 * @return the value returned by {@code body}
	 */
	public static <R> R runWith(@Nullable UUID queryId, Supplier<R> body) {
		try {
			return callWith(queryId, body::get);
		} catch (RuntimeException | Error e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * @return the bound UUID for the current scope, or {@link Optional#empty()} when no scope is active or the bound
	 *         value is {@code null}
	 */
	public static Optional<UUID> current() {
		if (CURRENT_QUERY_ID.isBound()) {
			return CURRENT_QUERY_ID.get();
		}
		return Optional.empty();
	}
}

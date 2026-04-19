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
package eu.solven.adhoc.map.factory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.factory.ASliceFactory.IHasEntries;
import eu.solven.adhoc.query.cube.IGroupBy;

/**
 * Enable building {@link Map} and {@link ISlice} in Adhoc context.
 * 
 * In Adhoc, we generate tons of {@link Map}-like for a given {@link IGroupBy}. Which means a tons of {@link Map}-like
 * for a predefined keySet. Given {@link Map} may be sorted, to enable faster merging (see
 * {@link IDagBottomUpStrategy}).
 * 
 * @author Benoit Lacelle
 */
public interface ISliceFactory {

	/**
	 * BEWARE The input {@link Iterable} must be sequenced. Typically, `Set.of` is rejected as not sequenced.
	 * 
	 * @param keys
	 * @return a {@link IMapBuilderPreKeys} for given set of keys.
	 */
	IMapBuilderPreKeys newMapBuilder(Iterable<? extends String> keys);

	/**
	 * 
	 * @param keys
	 * @return a {@link IMapBuilderPreKeys} for given set of keys.
	 */
	default IMapBuilderPreKeys newMapBuilder(String... keys) {
		return newMapBuilder(ImmutableList.copyOf(keys));
	}

	@Deprecated(since = "not used anymore", forRemoval = true)
	IAdhocMap buildMap(IHasEntries hasEntries);

	/**
	 * Runs {@code body} with any per-thread scopes required by this slice factory's backing storage. The default is a
	 * no-op (just invokes the body). Implementations backed by a scoped resource (e.g.
	 * {@link eu.solven.adhoc.encoding.page.ScopedValueAppendableTable}) override this to bind their scope around the
	 * body. Every dispatch site that pushes work onto a fresh thread (e.g. a virtual-thread {@code supplyAsync}) should
	 * wrap its submitted task with {@link #callWithScope} / {@link #runWithScope}.
	 *
	 * @return the value returned by {@code body}.
	 */
	// Mirrors Callable.call's `throws Exception` to propagate the body's checked exceptions transparently.
	@Deprecated(since = "Unclear design")
	@SuppressWarnings("PMD.SignatureDeclareThrowsException")
	default <R> R callWithScope(Callable<R> body) throws Exception {
		return body.call();
	}

	/**
	 * Runnable variant of {@link #callWithScope(Callable)}.
	 */
	@Deprecated(since = "Unclear design")
	default void runWithScope(Runnable body) {
		getWithScope(() -> {
			body.run();
			return null;
		});
	}

	/**
	 * Supplier variant of {@link #callWithScope(Callable)} — returns a value without declaring checked exceptions. Any
	 * checked exception thrown by {@link #callWithScope} (should not happen for a {@link Supplier} body) is rewrapped
	 * as {@link IllegalStateException}.
	 *
	 * @return the value returned by {@code body}.
	 */
	@Deprecated(since = "Unclear design")
	default <R> R getWithScope(Supplier<R> body) {
		try {
			return callWithScope(body::get);
		} catch (RuntimeException | Error e) {
			throw e;
		} catch (Exception e) {
			// Unreachable for a Supplier body, but protects against custom callWithScope overrides that wrap the
			// body in a way that exposes checked exceptions.
			throw new IllegalStateException(e);
		}
	}

}

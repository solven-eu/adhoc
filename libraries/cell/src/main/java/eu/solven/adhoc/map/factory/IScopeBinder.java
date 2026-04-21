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
package eu.solven.adhoc.map.factory;

import java.util.concurrent.Callable;

/**
 * Binds any per-thread scopes required by a scoped resource (e.g. a {@code ScopedValue}-backed storage layer) around a
 * body. {@link ISliceFactory} implementations that need per-thread scope binding (typically a backing
 * {@code ScopedValueAppendableTable}) should implement this interface — call sites then dispatch through
 * {@code PodExecutors} rather than reaching into {@link ISliceFactory} directly.
 *
 * The default flow for factories not implementing this interface is a plain no-op (just call the body).
 *
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IScopeBinder {

	/**
	 * Invokes {@code body} with any per-thread scopes required by this implementation bound.
	 *
	 * @param <R>
	 *            the body's return type
	 * @param body
	 *            the body to run
	 * @return the value returned by {@code body}
	 * @throws Exception
	 *             any checked exception propagated from {@code body}
	 */
	// Mirrors Callable.call's `throws Exception` so checked exceptions from the body propagate transparently.
	@SuppressWarnings("PMD.SignatureDeclareThrowsException")
	<R> R bindScope(Callable<R> body) throws Exception;

}

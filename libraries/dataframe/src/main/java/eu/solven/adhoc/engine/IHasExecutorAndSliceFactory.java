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
package eu.solven.adhoc.engine;

import com.google.common.util.concurrent.ListeningExecutorService;

import eu.solven.adhoc.map.factory.ISliceFactory;

/**
 * Marker for contexts exposing both a {@link ISliceFactory} (for building {@code IAdhocMap}s) and a
 * {@link ListeningExecutorService} (for dispatching parallel work). Implemented by {@link IAdhocFactories} and
 * {@code QueryPod} so helpers — notably {@link PodExecutors} — can accept either transparently and centralise
 * scope-binding around dispatched tasks.
 *
 * @author Benoit Lacelle
 */
public interface IHasExecutorAndSliceFactory {

	/**
	 * @return the slice factory used to allocate {@code IAdhocMap} instances.
	 */
	ISliceFactory getSliceFactory();

	/**
	 * @return the executor service onto which parallel work should be dispatched.
	 */
	ListeningExecutorService getExecutorService();

}

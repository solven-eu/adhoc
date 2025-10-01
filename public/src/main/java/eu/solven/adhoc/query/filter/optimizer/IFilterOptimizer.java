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
package eu.solven.adhoc.query.filter.optimizer;

import java.util.Collection;

import eu.solven.adhoc.query.filter.ISliceFilter;

/**
 * Holds the optimization of {@link ISliceFilter}
 * 
 * @author Benoit Lacelle
 */
public interface IFilterOptimizer {

	/**
	 * 
	 * @param filters
	 * @param willBeNegated
	 *            true if this expression will be negated (e.g. when being called by `OR`)
	 * @return
	 */
	ISliceFilter and(Collection<? extends ISliceFilter> filters, boolean willBeNegated);

	ISliceFilter or(Collection<? extends ISliceFilter> filters);

	ISliceFilter not(ISliceFilter first);

	/**
	 * Enable receiving event related to the optimization process.
	 * 
	 * @author Benoit Lacelle
	 */
	// Methods are defaulted to prevent breaking code when adding events
	interface IOptimizerEventListener {

		default void onOptimize(ISliceFilter filter) {
			// swallowed
		}

		default void onSkip(ISliceFilter filter) {
			// swallowed
		}
	}
}

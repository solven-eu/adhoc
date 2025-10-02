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
package eu.solven.adhoc.engine.tabular.optimizer;

import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.optimizer.FilterOptimizer;
import eu.solven.adhoc.query.filter.optimizer.FilterOptimizerWithCache;
import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;

/**
 * Helps creating a {@link IFilterOptimizer}.
 * 
 * We need a factory as some {@link IFilterOptimizerFactory} should be used only within a given context (e.g. to take
 * advantage of a cache, which must not be polluted by {@link ISliceFilter} irrelevant to given context, for
 * cache-efficiency purposes).
 * 
 * @author Benoit Lacelle
 */
public interface IFilterOptimizerFactory {
	IFilterOptimizer makeOptimizer();

	IFilterOptimizer makeOptimizerWithCache();

	static IFilterOptimizerFactory standard() {
		return new IFilterOptimizerFactory() {

			@Override
			public IFilterOptimizer makeOptimizer() {
				return FilterOptimizer.builder().build();
			}

			@Override
			public IFilterOptimizer makeOptimizerWithCache() {
				return FilterOptimizerWithCache.builder().build();
			}

		};
	}
}

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
package eu.solven.adhoc.filter;

import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.filter.stripper.IFilterStripperFactory;

/**
 * Groups the query-scoped filter-processing tools: a contextualised {@link IFilterStripperFactory} and a cached
 * {@link IFilterOptimizer}. Both are created once per query execution and shared across the sub-strategies that
 * participate in that execution.
 *
 * <p>
 * Use {@link IFilterFactories#makeQueryBundle()} to obtain an instance from the long-lived configuration-time
 * factories.
 *
 * @author Benoit Lacelle
 */
public interface IFilterQueryBundle {

	/**
	 * Returns the query-scoped {@link IFilterStripperFactory}. It is typically a lambda wrapping a shared base
	 * {@link eu.solven.adhoc.filter.stripper.IFilterStripper} that carries {@link ISliceFilter#MATCH_ALL} as the root
	 * WHERE clause.
	 *
	 * @return the query-scoped stripper factory
	 */
	IFilterStripperFactory getFilterStripperFactory();

	/**
	 * Returns the query-scoped {@link IFilterOptimizer}, which is expected to carry a per-query cache for deduplication
	 * of AND/OR/NOT optimisations performed during the same execution.
	 *
	 * @return the query-scoped optimizer
	 */
	IFilterOptimizer getFilterOptimizer();

	/**
	 * Creates an {@link IFilterQueryBundle} from the two query-scoped instances.
	 *
	 * @param stripperFactory
	 *            the contextualised stripper factory
	 * @param optimizer
	 *            the cached optimizer
	 * @return an immutable bundle wrapping both instances
	 */
	static IFilterQueryBundle of(IFilterStripperFactory stripperFactory, IFilterOptimizer optimizer) {
		return new IFilterQueryBundle() {
			@Override
			public IFilterStripperFactory getFilterStripperFactory() {
				return stripperFactory;
			}

			@Override
			public IFilterOptimizer getFilterOptimizer() {
				return optimizer;
			}
		};
	}
}

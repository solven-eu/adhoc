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
import eu.solven.adhoc.filter.optimizer.IFilterOptimizerFactory;
import eu.solven.adhoc.filter.stripper.IFilterStripperFactory;

/**
 * Configuration-time coupling of the two filter-related factories: {@link IFilterStripperFactory} and
 * {@link IFilterOptimizerFactory}. These factories are long-lived (e.g. Spring beans or static defaults) and are
 * injected together wherever both are needed at construction time.
 *
 * <p>
 * At query-execution time, call {@link #makeQueryBundle()} to derive a query-scoped {@link IFilterQueryBundle}
 * (contextualised stripper factory + cached optimizer) that is passed down to sub-strategies.
 *
 * @author Benoit Lacelle
 */
public interface IFilterFactories {

	/**
	 * Returns the long-lived {@link IFilterStripperFactory} used to create per-context
	 * {@link eu.solven.adhoc.filter.stripper.IFilterStripper} instances.
	 *
	 * @return the stripper factory
	 */
	IFilterStripperFactory getFilterStripperFactory();

	/**
	 * Returns the long-lived {@link IFilterOptimizerFactory} used to create per-query {@link IFilterOptimizer}
	 * instances (with or without a cache).
	 *
	 * @return the optimizer factory
	 */
	IFilterOptimizerFactory getFilterOptimizerFactory();

	/**
	 * Creates a query-scoped {@link IFilterQueryBundle}: a contextualised stripper factory (rooted at
	 * {@link ISliceFilter#MATCH_ALL}) and a cached {@link IFilterOptimizer}. Both instances are created fresh for each
	 * query execution and must not be shared across concurrent queries.
	 *
	 * @return a new {@link IFilterQueryBundle} for a single query execution
	 */
	default IFilterQueryBundle makeQueryBundle() {
		IFilterStripperFactory queryStripperFactory =
				getFilterStripperFactory().makeFilterStripper(ISliceFilter.MATCH_ALL)::withWhere;
		IFilterOptimizer optimizer = getFilterOptimizerFactory().makeOptimizerWithCache();
		return IFilterQueryBundle.of(queryStripperFactory, optimizer);
	}
}

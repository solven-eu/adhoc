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

import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.query.InternalQueryOptions;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;
import lombok.RequiredArgsConstructor;

/**
 * Standard implementation for {@link ITableQueryOptimizerFactory}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
public class TableQueryOptimizerFactory implements ITableQueryOptimizerFactory {

	protected IFilterOptimizer makeFilterOptimizer(AdhocFactories factories) {
		// WithCache as this optimize will be used for a single query
		return factories.getFilterOptimizerFactory().makeOptimizerWithCache();
	}

	@Override
	public ITableQueryOptimizer makeOptimizer(AdhocFactories factories, IHasQueryOptions hasOptions) {
		if (hasOptions.getOptions().contains(InternalQueryOptions.DISABLE_AGGREGATOR_INDUCTION)) {
			IFilterOptimizer filterOptimizer = makeFilterOptimizer(factories);
			return new TableQueryOptimizerNone(factories, filterOptimizer);
		} else {
			return makeOptimizer(factories);
		}
	}

	protected ITableQueryOptimizer makeOptimizer(AdhocFactories factories) {
		// WithCache as this optimize will be used for a single query
		IFilterOptimizer filterOptimizer = makeFilterOptimizer(factories);
		return new TableQueryOptimizer(factories, filterOptimizer);
	}

}

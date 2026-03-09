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

import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.tabular.splitter.ITableStepsGrouper;
import eu.solven.adhoc.engine.tabular.splitter.ITableStepsSplitter;
import eu.solven.adhoc.engine.tabular.splitter.InduceByAdhoc;
import eu.solven.adhoc.engine.tabular.splitter.InduceByGroupingSets;
import eu.solven.adhoc.engine.tabular.splitter.TableStepsGrouper;
import eu.solven.adhoc.engine.tabular.splitter.TableStepsGrouperByAggregator;
import eu.solven.adhoc.engine.tabular.splitter.TableStepsGrouperNoGroup;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.query.InternalQueryOptions;
import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Standard implementation for {@link ITableQueryOptimizerFactory}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@RequiredArgsConstructor
public class TableQueryOptimizerFactory implements ITableQueryOptimizerFactory {

	protected IFilterOptimizer makeFilterOptimizer(IAdhocFactories factories) {
		// WithCache as this optimizer will be used for a single query
		return factories.getFilterOptimizerFactory().makeOptimizerWithCache();
	}

	@Override
	public ITableQueryOptimizer makeOptimizer(IAdhocFactories factories, IHasQueryOptions hasOptions) {
		ITableStepsSplitter splitter;
		if (hasOptions.getOptions().contains(InternalQueryOptions.INDUCE_BY_ADHOC)) {
			splitter = new InduceByAdhoc();
		} else if (hasOptions.getOptions().contains(InternalQueryOptions.INDUCE_BY_TABLE)) {
			splitter = new InduceByGroupingSets();
		} else {
			// BEWARE We're unclear about the right defaults
			splitter = new InduceByGroupingSets();
			log.debug("Default {} led to {}", ITableStepsSplitter.class.getName(), splitter.getClass().getName());
		}

		ITableStepsGrouper grouper;
		if (hasOptions.getOptions().contains(InternalQueryOptions.TABLEQUERY_PER_OPTIONS)) {
			grouper = new TableStepsGrouper();
		} else if (hasOptions.getOptions().contains(InternalQueryOptions.TABLEQUERY_PER_AGGREGATOR)) {
			grouper = new TableStepsGrouperByAggregator();
		} else if (hasOptions.getOptions().contains(InternalQueryOptions.TABLEQUERY_PER_STEPS)) {
			grouper = new TableStepsGrouperNoGroup();
		} else {
			// BEWARE We're unclear about the right defaults
			grouper = new TableStepsGrouper();
			log.debug("Default {} led to {}", ITableStepsGrouper.class.getName(), grouper.getClass().getName());
		}

		// WithCache as this optimize will be used for a single query
		IFilterOptimizer filterOptimizer = makeFilterOptimizer(factories);

		return new TableQueryOptimizer(factories, filterOptimizer, splitter, grouper);
	}

}

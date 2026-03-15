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
import eu.solven.adhoc.engine.tabular.splitter.TableStepsGrouperByAffinity;
import eu.solven.adhoc.engine.tabular.splitter.TableStepsGrouperByAggregator;
import eu.solven.adhoc.engine.tabular.splitter.TableStepsGrouperNoGroup;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.query.InternalQueryOptions;
import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Standard implementation for {@link ITableQueryFactoryFactory}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@RequiredArgsConstructor
public class TableQueryFactoryFactory implements ITableQueryFactoryFactory {

	@Override
	public ITableQueryFactory makeOptimizer(IAdhocFactories factories,
			IFilterOptimizer filterOptimizer,
			IHasQueryOptions hasOptions) {
		ITableStepsSplitter splitter = makeSplitter(hasOptions);

		ITableStepsGrouper grouper = makeGrouper(hasOptions, splitter);

		return new TableQueryFactory(factories, filterOptimizer, splitter, grouper);
	}

	protected ITableStepsGrouper makeGrouper(IHasQueryOptions hasOptions) {
		return makeGrouper(hasOptions, null);
	}

	/**
	 * Selects the {@link ITableStepsGrouper} for the given query options and splitter. When the splitter is
	 * {@link InduceByGroupingSets}, defaults to {@link TableStepsGrouperByAffinity} to avoid cartesian-product waste in
	 * GROUPING SETS queries.
	 */
	protected ITableStepsGrouper makeGrouper(IHasQueryOptions hasOptions, ITableStepsSplitter splitter) {
		ITableStepsGrouper grouper;
		if (hasOptions.getOptions().contains(InternalQueryOptions.TABLEQUERY_PER_OPTIONS)) {
			grouper = new TableStepsGrouper();
		} else if (hasOptions.getOptions().contains(InternalQueryOptions.TABLEQUERY_PER_AGGREGATOR)) {
			grouper = new TableStepsGrouperByAggregator();
		} else if (hasOptions.getOptions().contains(InternalQueryOptions.TABLEQUERY_PER_STEPS)) {
			grouper = new TableStepsGrouperNoGroup();
		} else if (hasOptions.getOptions().contains(InternalQueryOptions.TABLEQUERY_PER_AFFINITY)) {
			grouper = new TableStepsGrouperByAffinity();
		} else if (splitter instanceof InduceByGroupingSets) {
			// Tightly coupled: InduceByGroupingSets makes every step a leaf, so without an affinity grouper the
			// resulting TableQueryV3 would contain the full cartesian product of measures × groupBys.
			grouper = new TableStepsGrouperByAffinity();
			log.debug("InduceByGroupingSets splitter led to default grouper {}", grouper.getClass().getName());
		} else {
			// BEWARE We're unclear about the right defaults
			grouper = new TableStepsGrouper();
			log.debug("Default {} led to {}", ITableStepsGrouper.class.getName(), grouper.getClass().getName());
		}
		return grouper;
	}

	protected ITableStepsSplitter makeSplitter(IHasQueryOptions hasOptions) {
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
		return splitter;
	}

}

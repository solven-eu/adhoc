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
package eu.solven.adhoc.engine.tabular.inducer;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.SplitTableQueries;
import eu.solven.adhoc.engine.tabular.splitter.InducerHelpers;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.options.IHasQueryOptions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Standard {@link ITableQueryInducer}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class TableQueryInducer implements ITableQueryInducer {
	final IAdhocFactories factories;

	@Getter
	protected final IFilterOptimizer filterOptimizer;

	final IInducedEvaluator inducedEvaluator;

	@Deprecated(since = "For unit-tests, else you should probably re-use a filterOptimizer")
	public TableQueryInducer(IAdhocFactories factories) {
		this(factories, factories.getFilterOptimizerFactory().makeOptimizer());
	}

	public TableQueryInducer(IAdhocFactories factories, IFilterOptimizer filterOptimizer) {
		this.factories = factories;
		if (filterOptimizer == null) {
			this.filterOptimizer = factories.getFilterOptimizerFactory().makeOptimizer();
		} else {
			this.filterOptimizer = filterOptimizer;
		}

		this.inducedEvaluator = StandardInducedEvaluatorFactory.builder()
				.factories(factories)
				.filterOptimizer(this.filterOptimizer)
				.build()
				.build();
	}

	@Override
	public IMultitypeMergeableColumn<ISlice> evaluateInduced(IHasQueryOptions hasOptions,
			SplitTableQueries inducerAndInduced,
			Map<TableQueryStep, ICuboid> stepToValues,
			TableQueryStep induced) {
		List<TableQueryStep> inducers = inducerAndInduced.getInducers(induced);

		Map<TableQueryStep, ICuboid> inducerToCuboid =
				inducers.stream().collect(ImmutableMap.toImmutableMap(Function.identity(), stepToValues::get));

		// Rely on the inducer with the smaller number of rows, as it's a good heuristic of being the most processed one
		// for our own cuboic.
		Map.Entry<TableQueryStep, ICuboid> smallest = inducerToCuboid.entrySet()
				.stream()
				.max(Comparator.comparingLong(e -> e.getValue().size()))
				.orElseThrow(() -> new IllegalStateException("No inducer for induced=%s".formatted(induced)));

		TableQueryStep inducer = smallest.getKey();
		ICuboid inducerValues = smallest.getValue();

		Aggregator aggregator = inducer.getMeasure();
		IAggregation aggregation = factories.getOperatorFactory().makeAggregation(aggregator);

		Collection<IAdhocColumn> inducerColumns = inducer.getGroupBy().getColumns();
		Optional<ISliceFilter> optSliceFilter =
				InducerHelpers.makeLeftoverFilter(inducerColumns, inducer.getFilter(), induced.getFilter());
		if (optSliceFilter.isEmpty()) {
			throw new IllegalStateException(
					"Can not make a leftover filter given inducer=%s and induced=%s".formatted(inducer, induced));
		}

		ISliceFilter sliceFilter = optSliceFilter.get();

		IMultitypeMergeableColumn<ISlice> inducedValues =
				inducedEvaluator.tryEvaluate(inducerValues, inducer, induced, sliceFilter, aggregation, aggregator)
						.orElseThrow(() -> new IllegalStateException(
								"No evaluator succeeded for inducer=%s induced=%s".formatted(inducer, induced)));

		if (hasOptions.isDebugOrExplain()) {
			Set<String> removedGroupBys =
					Sets.difference(inducer.getGroupBy().getSortedColumns(), induced.getGroupBy().getSortedColumns());
			log.info(
					"[EXPLAIN] size={} induced size={} on agg={} by filtering f={} and reducing groupBy={} ({} induced {})",
					inducerValues.size(),
					inducedValues.size(),
					aggregator.getName(),
					sliceFilter,
					removedGroupBys,
					inducer,
					induced);
		}

		return inducedValues;
	}
}

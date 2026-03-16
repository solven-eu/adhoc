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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.data.column.ICuboid;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.IColumnFactory;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.filter.FilterUtility;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.filter.stripper.IFilterStripper;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.transformator.step.CombinatorQueryStep;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV3;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Behavior shared by most {@link ITableQueryFactory}
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public abstract class ATableQueryFactory implements ITableQueryFactory, IHasFilterOptimizer {
	protected final IAdhocFactories factories;

	@Getter
	protected final IFilterOptimizer filterOptimizer;

	protected final FilterUtility filterHelper;

	protected Supplier<FilterUtility> filterUtility =
			Suppliers.memoize(() -> FilterUtility.builder().optimizer(getFilterOptimizer()).build());

	@Deprecated(since = "For unit-tests, else you should probably re-use a filterOptimizer")
	public ATableQueryFactory(IAdhocFactories factories) {
		this(factories, factories.getFilterOptimizerFactory().makeOptimizer());
	}

	public ATableQueryFactory(IAdhocFactories factories, IFilterOptimizer filterOptimizer) {
		this.factories = factories;
		if (filterOptimizer == null) {
			this.filterOptimizer = factories.getFilterOptimizerFactory().makeOptimizer();
		} else {
			this.filterOptimizer = filterOptimizer;
		}

		this.filterHelper = FilterUtility.builder().optimizer(this.filterOptimizer).build();
	}

	protected TableQueryV3 makeTableQuery(CubeQueryStep context, Collection<CubeQueryStep> steps) {
		Set<IGroupBy> groupBys = steps.stream().map(CubeQueryStep::getGroupBy).collect(ImmutableSet.toImmutableSet());

		// This is the filter applicable to all aggregators: it will be applied in WHERE
		Set<ISliceFilter> filters = steps.stream().map(CubeQueryStep::getFilter).collect(ImmutableSet.toImmutableSet());
		ISliceFilter commonFilter = filterUtility.get().commonAnd(filters);
		IFilterStripper stripper = factories.getFilterStripperFactory().makeFilterStripper(commonFilter);

		// Strip the WHERE from each individual FILTER
		Set<FilteredAggregator> strippedAggregators = steps.stream().map(step -> {
			ISliceFilter strippedFromWhere = stripper.strip(step.getFilter());

			return FilteredAggregator.builder()
					.aggregator((Aggregator) step.getMeasure())
					// transfer the stripped filter as `FILTER`
					.filter(strippedFromWhere)
					.build();
		}).collect(ImmutableSet.toImmutableSet());

		Map<String, List<FilteredAggregator>> aliasToAggregators = strippedAggregators.stream()
				.collect(Collectors.groupingBy(FilteredAggregator::getAlias, LinkedHashMap::new, Collectors.toList()));

		// Ensure each aggregator has a different name even if they rely on the same column
		List<FilteredAggregator> aliasedAggregators = aliasToAggregators.entrySet().stream().flatMap(e -> {
			List<FilteredAggregator> aggregators = e.getValue();

			if (aggregators.size() == 1) {
				// no conflict
				return Stream.of(aggregators.getFirst());
			} else {
				AtomicInteger aliasIndex = new AtomicInteger();
				return aggregators.stream().map(a -> a.toBuilder().index(aliasIndex.getAndIncrement()).build());
			}
		}).toList();

		return TableQueryV3.edit(context)
				.groupBys(groupBys)
				.aggregators(aliasedAggregators)
				.filter(commonFilter)
				.build();
	}

	protected IMultitypeMergeableColumn<IAdhocSlice> prepareInducedColumn(CubeQueryStep inducer,
			CubeQueryStep induced,
			ICuboid inducerValues,
			IAggregation aggregation) {
		NavigableSet<String> inducerColumns = inducer.getGroupBy().getGroupedByColumns();
		NavigableSet<String> inducedColumns = induced.getGroupBy().getGroupedByColumns();
		boolean doesBreakSorting = breakSorting(inducerColumns, inducedColumns);

		IMultitypeMergeableColumn<IAdhocSlice> inducedValues;
		int capacity = CombinatorQueryStep.sumSizes(ImmutableSet.of(inducerValues));
		IColumnFactory columnFactory = factories.getColumnFactory();

		if (doesBreakSorting) {
			log.debug("random-insert for {} -> {}", inducerColumns, inducedColumns);
			inducedValues = columnFactory.makeColumnRandomInsertions(aggregation, capacity);
		} else {
			log.debug("sorted-insert for {} -> {}", inducerColumns, inducedColumns);
			inducedValues = columnFactory.makeColumn(aggregation, capacity);
		}

		return inducedValues;
	}

	/**
	 * 
	 * @param inducer
	 * @param induced
	 * @return true of induced order is not naturally derived from inducer order
	 */
	// `a,b,c` is maintained by `a`, `a,b`
	// `a,b,c` is broken by `b`, `a,c`
	// TODO This question the ordering of slice in Adhoc. IAdhocMap has an ordering based on key lexicographical order.
	// But we may sort keys based on their expected variance, or based on the actual query.
	protected boolean breakSorting(NavigableSet<String> inducer, NavigableSet<String> induced) {
		List<String> inducerAsList = inducer.stream().limit(induced.size()).toList();
		List<String> inducedAsList = induced.stream().toList();

		return !inducerAsList.equals(inducedAsList);
	}

	protected IAdhocSlice inducedGroupBy(NavigableSet<String> groupedByColumns, IAdhocSlice inducer) {
		return inducer.retainAll(groupedByColumns);
	}

}

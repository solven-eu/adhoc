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

import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import com.google.common.collect.Sets;

import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.map.AdhocMap;
import eu.solven.adhoc.map.AdhocMap.AdhocMapBuilder;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.decomposition.DecompositionHelpers;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.query.filter.FilterMatcher;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV2;
import lombok.extern.slf4j.Slf4j;

/**
 * Behavior shared by most {@link ITableQueryOptimizer}
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public abstract class ATableQueryOptimizer implements ITableQueryOptimizer {

	/**
	 * Check everything representing the context of the query. Typically represents the {@link IQueryOption} and the
	 * customMarker.
	 * 
	 * @param inducer
	 * @return a CubeQueryStep which has been fleshed-out of what's not the query context.
	 */
	protected CubeQueryStep contextOnly(CubeQueryStep inducer) {
		return CubeQueryStep.edit(inducer)
				.measure("noMeasure")
				.groupBy(IAdhocGroupBy.GRAND_TOTAL)
				.filter(IAdhocFilter.MATCH_ALL)
				.build();
	}

	@Override
	public Set<TableQueryV2> groupByEnablingFilterPerMeasure(Set<CubeQueryStep> tableQueries) {
		// TODO Add option to skip FILTER optimizations
		return TableQueryV2.fromV1(TableQuery.fromSteps(tableQueries));
	}

	@Override
	public IMultitypeMergeableColumn<SliceAsMap> evaluateInduced(AdhocFactories factories,
			IHasQueryOptions hasOptions,
			SplitTableQueries inducerAndInduced,
			Map<CubeQueryStep, ISliceToValue> stepToValues,
			CubeQueryStep induced) {
		List<CubeQueryStep> inducers = inducerAndInduced.getDependencies(induced);
		if (inducers.size() != 1) {
			throw new IllegalStateException(
					"Induced should have a single inducer. induced=%s inducers=%s".formatted(induced, inducers));
		}

		CubeQueryStep inducer = inducers.getFirst();
		ISliceToValue inducerValues = stepToValues.get(inducer);

		Aggregator aggregator = (Aggregator) inducer.getMeasure();
		IAggregation aggregation = factories.getOperatorFactory().makeAggregation(aggregator);
		IMultitypeMergeableColumn<SliceAsMap> inducedValues =
				factories.getColumnsFactory().makeColumn(aggregation, List.of(inducerValues));

		FilterMatcher filterMatcher = FilterMatcher.builder()
				.filter(induced.getFilter())
				// A filtered column may be missing from groupBy: it is a common filter from induced and inducer: it is
				// valid.
				.onMissingColumn(DecompositionHelpers.onMissingColumn())
				.build();
		NavigableSet<String> inducedColumns = induced.getGroupBy().getGroupedByColumns();

		inducerValues.stream().filter(s -> {
			return filterMatcher.match(s.getSlice().getCoordinates());
		}).forEach(slice -> {
			// inducer have same or more columns than induced
			SliceAsMap inducedGroupBy = inducedGroupBy(inducedColumns, slice.getSlice());
			slice.getValueProvider().acceptReceiver(inducedValues.merge(inducedGroupBy));
		});

		if (hasOptions.isDebugOrExplain()) {
			Set<String> removedGroupBys = Sets.difference(inducer.getGroupBy().getGroupedByColumns(),
					induced.getGroupBy().getGroupedByColumns());
			log.info("[EXPLAIN] size={} induced size={} by removing groupBy={} ({} induced {})",
					inducerValues.size(),
					inducedValues.size(),
					removedGroupBys,
					inducer,
					induced);
		}

		return inducedValues;
	}

	protected SliceAsMap inducedGroupBy(NavigableSet<String> groupedByColumns, SliceAsMap inducer) {
		AdhocMapBuilder induced = AdhocMap.builder(groupedByColumns);

		groupedByColumns.forEach(inducedColumn -> {
			induced.append(inducer.getRawSliced(inducedColumn));
		});

		return SliceAsMap.fromMap(induced.build());
	}

}

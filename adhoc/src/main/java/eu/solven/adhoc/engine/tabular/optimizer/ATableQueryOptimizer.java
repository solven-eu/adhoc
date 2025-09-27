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
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.transformator.step.CombinatorQueryStep;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.FilterMatcher;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.NotFilter;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV2;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Behavior shared by most {@link ITableQueryOptimizer}
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@RequiredArgsConstructor
public abstract class ATableQueryOptimizer implements ITableQueryOptimizer {
	final AdhocFactories factories;

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
				.filter(ISliceFilter.MATCH_ALL)
				.build();
	}

	@Override
	public Set<TableQueryV2> groupByEnablingFilterPerMeasure(Set<CubeQueryStep> tableQueries) {
		// TODO Add option to skip FILTER optimizations
		return TableQueryV2.fromV1(TableQuery.fromSteps(tableQueries));
	}

	protected Optional<ISliceFilter> makeLeftoverFilter(Collection<IAdhocColumn> inducerColumns,
			ISliceFilter inducerFilter,
			ISliceFilter inducedFilter) {
		// BEWARE There is two different ways to filters rows from inducer for induced:
		// Either we reject the rows which are in inducer but not expected by induced
		// Or we keep-only the rows in inducer given additional constraints in induced
		// We'll choose one or the other depending on columns available in inducer

		Optional<ISliceFilter> leftoverFilter = Optional.empty();

		ISliceFilter commonFilter = FilterHelpers.commonAnd(ImmutableSet.of(inducerFilter, inducedFilter));
		ISliceFilter inducedLeftoverFilter = FilterHelpers.stripWhereFromFilter(commonFilter, inducedFilter);

		// This match the row which has to be kept from inducer for induced
		boolean hasLeftoverFilteringColumns = inducerColumns.stream()
				.map(IAdhocColumn::getName)
				.toList()
				.containsAll(FilterHelpers.getFilteredColumns(inducedLeftoverFilter));

		if (hasLeftoverFilteringColumns) {
			leftoverFilter = Optional.of(inducedLeftoverFilter);
		}

		// This match the rows in the inducer which should be stripped off the induced
		ISliceFilter rejectingFilter = FilterBuilder.and(inducerFilter, NotFilter.not(inducedFilter)).optimize();
		boolean hasRejectingColumns = inducerColumns.stream()
				.map(IAdhocColumn::getName)
				.toList()
				.containsAll(FilterHelpers.getFilteredColumns(rejectingFilter));

		if (hasRejectingColumns) {
			leftoverFilter = Optional.of(NotFilter.not(rejectingFilter));
		}
		return leftoverFilter;
	}

	@Override
	public IMultitypeMergeableColumn<IAdhocSlice> evaluateInduced(IHasQueryOptions hasOptions,
			SplitTableQueries inducerAndInduced,
			Map<CubeQueryStep, ISliceToValue> stepToValues,
			CubeQueryStep induced) {
		List<CubeQueryStep> inducers = inducerAndInduced.getInducers(induced);
		if (inducers.size() != 1) {
			throw new IllegalStateException(
					"Induced should have a single inducer. induced=%s inducers=%s".formatted(induced, inducers));
		}

		CubeQueryStep inducer = inducers.getFirst();
		ISliceToValue inducerValues = stepToValues.get(inducer);

		Aggregator aggregator = (Aggregator) inducer.getMeasure();
		IAggregation aggregation = factories.getOperatorFactory().makeAggregation(aggregator);
		IMultitypeMergeableColumn<IAdhocSlice> inducedValues = factories.getColumnFactory()
				.makeColumn(aggregation, CombinatorQueryStep.sumSizes(Set.of(inducerValues)));

		Collection<IAdhocColumn> inducerColumns = inducer.getGroupBy().getNameToColumn().values();
		Optional<ISliceFilter> sliceFilter =
				makeLeftoverFilter(inducerColumns, inducer.getFilter(), induced.getFilter());
		if (sliceFilter.isEmpty()) {
			throw new IllegalStateException(
					"Can not make a leftover filter given inducer=%s and induced=%s".formatted(inducer, induced));
		}

		FilterMatcher filterMatcher = FilterMatcher.builder()
				.filter(sliceFilter.get())
				.onMissingColumn(FilterMatcher.failOnMissing())
				.build();
		NavigableSet<String> inducedColumns = induced.getGroupBy().getGroupedByColumns();

		inducerValues.stream()
				// filter the relevant rows from inducer
				.filter(s -> filterMatcher.match(s.getSlice()))
				// aggregate the accepted rows
				.forEach(slice -> {
					// inducer have same or more columns than induced
					IAdhocSlice inducedGroupBy = inducedGroupBy(inducedColumns, slice.getSlice());
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

	protected IAdhocSlice inducedGroupBy(NavigableSet<String> groupedByColumns, IAdhocSlice inducer) {
		var induced = factories.getSliceFactory().newMapBuilder(groupedByColumns);

		groupedByColumns.forEach(inducedColumn -> {
			induced.append(inducer.getRawSliced(inducedColumn));
		});

		return induced.build().asSlice();
	}

}

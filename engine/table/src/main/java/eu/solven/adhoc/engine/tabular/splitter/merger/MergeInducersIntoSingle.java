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
package eu.solven.adhoc.engine.tabular.splitter.merger;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.engine.dag.GraphHelpers;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.step.ACubeQueryStep;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.FilterUtility;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.filter.stripper.IFilterStripper;
import eu.solven.adhoc.filter.stripper.IFilterStripperFactory;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Given a set of inducers, this will generate an additional set of inducers.
 * 
 * The point is typically to reduce the number of inducers.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class MergeInducersIntoSingle implements IMergeInducers {

	@Default
	@NonNull
	protected final IFilterStripperFactory filterStripperFactory =
			AdhocFactoriesUnsafe.factories.getFilterStripperFactory();

	@Default
	@NonNull
	protected final IFilterOptimizer filterOptimizer =
			AdhocFactoriesUnsafe.factories.getFilterOptimizerFactory().makeOptimizer();

	public static IMergeInducersFactory makeFactory() {
		return filterBundle -> MergeInducersIntoSingle.builder()
				.filterStripperFactory(filterBundle.getFilterStripperFactory())
				.filterOptimizer(filterBundle.getFilterOptimizer())
				.build();
	}

	@Override
	public IAdhocDag<TableQueryStep> mergeInducers(TableQueryStep contextualAggregator, Set<TableQueryStep> steps) {
		if (steps.size() <= 1) {
			// Nothing to merge
			return GraphHelpers.makeGraph();
		}

		IGroupBy mergedGroupBy = mergeGroupBy(steps);

		// OR between each inducer own filter induced will fetch the union of rows for all induced
		Set<ISliceFilter> filters =
				steps.stream().map(TableQueryStep::getFilter).collect(ImmutableSet.toImmutableSet());
		ISliceFilter combinedOr = FilterBuilder.or(filters).optimize(filterOptimizer);

		TableQueryStep inducer = contextualAggregator.toBuilder().filter(combinedOr).groupBy(mergedGroupBy).build();

		if (contextualAggregator.isDebugOrExplain()) {
			log.info("[EXPLAIN] a={} merged={} steps, groupBy column coverage: [{}]",
					contextualAggregator,
					steps.size(),
					PepperLogHelper.lazyToString(() -> buildColumnCoverageString(steps, mergedGroupBy)));
		}

		IAdhocDag<TableQueryStep> inducedToInducer = GraphHelpers.makeGraph();
		if (steps.contains(inducer)) {
			return inducedToInducer;
		} else {
			inducedToInducer.addVertex(inducer);
			steps.forEach(inducedToInducer::addVertex);

			steps.forEach(step -> inducedToInducer.addEdge(step, inducer));

			return inducedToInducer;
		}

	}

	/**
	 * Builds a human-readable coverage string for EXPLAIN output. For each column in {@code mergedGroupBy}, reports
	 * what percentage of the input {@code steps} already carried that column — either in their own groupBy or in their
	 * filter. A column at 100 % was native to every step; a column below 100 % was added by the merger solely to
	 * support row-splitting.
	 *
	 * @return a comma-separated list of {@code col=P%(count/total)} entries, one per grouped-by column
	 */
	@SuppressWarnings("checkstyle:MagicNumber")
	protected String buildColumnCoverageString(Set<TableQueryStep> steps, IGroupBy mergedGroupBy) {
		int total = steps.size();
		return mergedGroupBy.getSortedColumns().stream().map(col -> {
			long count = steps.stream().filter(s -> ACubeQueryStep.getColumns(s).contains(col)).count();
			return "%s=%d%%(%d/%d)".formatted(col, count * 100 / total, count, total);
		}).collect(Collectors.joining(", "));
	}

	protected IGroupBy mergeGroupBy(Set<TableQueryStep> steps) {
		FilterUtility filterUtility = FilterUtility.builder().optimizer(filterOptimizer).build();

		// This relates to the steps executed by the table
		// In this case, we need to add a filtered column if it is not common to all steps, as it will be needed to
		// filter rows for given sub-steps
		ISliceFilter rawCommonFilter = filterUtility.commonAnd(steps.stream().map(TableQueryStep::getFilter).toList());
		ISliceFilter commonFilter = FilterBuilder.and(rawCommonFilter).optimize(filterOptimizer);
		IFilterStripper commonStripper = filterStripperFactory.makeFilterStripper(commonFilter);

		Set<ISliceFilter> eachInducedFilters = new LinkedHashSet<>();

		// collect the filters without the WHERE clause, in order to define which columns is needed for later splitting
		steps.forEach(step -> {
			// We need these additional columns for proper filtering
			ISliceFilter stripped = commonStripper.strip(step.getFilter());

			eachInducedFilters.add(stripped);
		});

		IGroupBy originalGroupBy = GroupByColumns.mergeNonAmbiguous(
				steps.stream().map(TableQueryStep::getGroupBy).collect(ImmutableSet.toImmutableSet()));
		ImmutableSet<String> columnsToDifferenciate = eachInducedFilters.stream()
				.flatMap(f -> FilterHelpers.getFilteredColumns(f).stream())
				.collect(ImmutableSet.toImmutableSet());
		Set<String> missingColumns = Sets.difference(columnsToDifferenciate, originalGroupBy.getSortedColumns());
		return GroupByColumns.builder()
				.columns(originalGroupBy.getColumns())
				.columns(missingColumns.stream().map(ReferencedColumn::ref).toList())
				.build();
	}

}

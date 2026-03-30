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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;

import eu.solven.adhoc.engine.step.ICubeQueryStep;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.filter.FilterUtility;
import eu.solven.adhoc.filter.IFilterQueryBundle;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.filter.stripper.IFilterStripper;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV4;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Behavior shared by most {@link ITableQueryFactory}
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public abstract class ATableQueryFactory implements ITableQueryFactory, IHasFilterOptimizer {
	@Getter
	protected final IFilterQueryBundle filterBundle;

	protected final FilterUtility filterHelper;

	protected Supplier<FilterUtility> filterUtility =
			Suppliers.memoize(() -> FilterUtility.builder().optimizer(getFilterOptimizer()).build());

	public ATableQueryFactory(IFilterQueryBundle filterBundle) {
		this.filterBundle = filterBundle;

		this.filterHelper = FilterUtility.builder().optimizer(this.filterBundle.getFilterOptimizer()).build();
	}

	@Override
	public IFilterOptimizer getFilterOptimizer() {
		return filterBundle.getFilterOptimizer();
	}

	/**
	 * Builds a {@link TableQueryV4} from the given steps, grouping them by {@link IGroupBy} so that each groupBy only
	 * carries the aggregators it actually needs. A shared WHERE filter is extracted from the common prefix of all step
	 * filters; individual FILTER clauses handle the remainder per aggregator.
	 */
	protected TableQueryV4 makeTableQueryV4(ICubeQueryStep context, Collection<? extends TableQueryStep> steps) {
		// This is the filter applicable to all aggregators: it will be applied in WHERE
		Set<ISliceFilter> filters =
				steps.stream().map(ICubeQueryStep::getFilter).collect(ImmutableSet.toImmutableSet());
		ISliceFilter commonFilter = filterUtility.get().commonAnd(filters);
		IFilterStripper stripper = filterBundle.getFilterStripperFactory().makeFilterStripper(commonFilter);

		// Group steps by their IGroupBy
		Map<IGroupBy, List<TableQueryStep>> byGroupBy = steps.stream()
				.collect(Collectors.groupingBy(ICubeQueryStep::getGroupBy, LinkedHashMap::new, Collectors.toList()));

		ImmutableSetMultimap.Builder<IGroupBy, FilteredAggregator> multimapBuilder = ImmutableSetMultimap.builder();
		byGroupBy.forEach((groupBy, groupBySteps) -> {
			// Strip the WHERE from each individual FILTER — each step is guaranteed to carry an Aggregator
			Set<FilteredAggregator> strippedAggregators = groupBySteps.stream().map(step -> {
				ISliceFilter strippedFromWhere = stripper.strip(step.getFilter());
				return FilteredAggregator.builder().aggregator(step.getMeasure()).filter(strippedFromWhere).build();
			}).collect(ImmutableSet.toImmutableSet());

			Map<String, List<FilteredAggregator>> aliasToAggregators = strippedAggregators.stream()
					.collect(Collectors
							.groupingBy(FilteredAggregator::getAlias, LinkedHashMap::new, Collectors.toList()));

			List<FilteredAggregator> aliasedAggregators = aliasToAggregators.entrySet().stream().flatMap(e -> {
				List<FilteredAggregator> aggregators = e.getValue();
				if (aggregators.size() == 1) {
					return Stream.of(aggregators.getFirst());
				} else {
					AtomicInteger aliasIndex = new AtomicInteger();
					return aggregators.stream().map(a -> a.toBuilder().index(aliasIndex.getAndIncrement()).build());
				}
			}).toList();

			multimapBuilder.putAll(groupBy, aliasedAggregators);
		});

		return TableQueryV4.builder()
				.filter(commonFilter)
				.groupByToAggregators(multimapBuilder.build())
				.customMarker(context.getCustomMarker())
				.options(context.getOptions())
				.build();
	}

}

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
package eu.solven.adhoc.query.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.options.IQueryOption;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.top.AdhocTopClause;
import eu.solven.adhoc.table.ITableWrapper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

/**
 * A query over an {@link ITableWrapper} that explicitly maps each {@link IGroupBy} to the exact
 * {@link FilteredAggregator}s required for it.
 *
 * <p>
 * Unlike {@link TableQueryV3}, which computes the full cartesian product of all groupBys and all aggregators, V4
 * carries only the (groupBy, aggregator) pairs that are actually needed. This eliminates wasteful computation when
 * different groupBys require different measures.
 *
 * <p>
 * V4 decomposes into one or more {@link TableQueryV3}s via {@link #streamV3()}: groupBys that share an identical
 * aggregator set are collapsed into a single GROUPING SET query, while the rest produce independent queries (combined
 * at the Java level as UNION ALL).
 *
 * @author Benoit Lacelle
 * @see TableQueryV3
 */
@Value
@Builder(toBuilder = true)
@SuppressWarnings("PMD.UnusedAssignment")
// https://blog.jooq.org/how-to-calculate-multiple-aggregate-functions-in-a-single-query/
public class TableQueryV4 implements ITableQuery {

	// a filter shared through all aggregators and all groupBys
	@Default
	ISliceFilter filter = ISliceFilter.MATCH_ALL;

	// The explicit mapping: which aggregators are required for each groupBy
	@NonNull
	ImmutableSetMultimap<IGroupBy, FilteredAggregator> groupByToAggregators;

	@Default
	Object customMarker = null;

	@Default
	AdhocTopClause topClause = AdhocTopClause.NO_LIMIT;

	@NonNull
	@Singular
	ImmutableSet<IQueryOption> options;

	protected TableQueryV4(ISliceFilter filter,
			Multimap<IGroupBy, FilteredAggregator> groupByToAggregators,
			Object customMarker,
			AdhocTopClause topClause,
			ImmutableSet<IQueryOption> options) {
		this.filter = filter;
		this.groupByToAggregators = ImmutableSetMultimap.copyOf(groupByToAggregators);
		this.customMarker = customMarker;
		this.topClause = topClause;
		this.options = options;
	}

	public static TableQueryV4.TableQueryV4Builder edit(TableQuery tableQuery) {
		return edit(TableQueryV2.edit(tableQuery).build());
	}

	public static TableQueryV4.TableQueryV4Builder edit(TableQueryV2 tableQuery) {
		return edit(TableQueryV3.edit(tableQuery).build());
	}

	public static TableQueryV4.TableQueryV4Builder edit(TableQueryV3 tableQuery) {
		// Like V2, but with a single groupBy
		return TableQueryV4.builder()
				.filter(tableQuery.getFilter())
				.customMarker(tableQuery.getCustomMarker())
				.options(tableQuery.getOptions())
				.topClause(tableQuery.getTopClause())
				.groupByToAggregators(tableQuery.streamGroupBy().collect(ImmutableSet.toImmutableSet()),
						tableQuery.getAggregators());
	}

	/**
	 * Decomposes this V4 into {@link TableQueryV3}s by grouping entries whose aggregator sets are identical into a
	 * single GROUPING SET. Entries with distinct aggregator sets each produce a separate V3, which the caller combines
	 * at the Java level (UNION ALL semantics).
	 */
	public Stream<TableQueryV3> streamV3() {
		// SetMultimap<FilteredAggregator, IGroupBy> aggSetToGroupBys =
		// MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();
		// Multimaps.invertFrom(groupByToAggregators, aggSetToGroupBys);

		// Group groupBys that share exactly the same aggregator set: they can share a GROUPING SET
		Map<ImmutableSet<FilteredAggregator>, List<IGroupBy>> aggSetToGroupBys = new LinkedHashMap<>();
		groupByToAggregators.keySet()
				.forEach(groupBy -> aggSetToGroupBys
						.computeIfAbsent(groupByToAggregators.get(groupBy), k -> new ArrayList<>())
						.add(groupBy));

		return aggSetToGroupBys.entrySet()
				.stream()
				.map(e -> TableQueryV3.builder()
						.filter(filter)
						.groupBys(e.getValue())
						.aggregators(e.getKey())
						.customMarker(customMarker)
						.topClause(topClause)
						.options(options)
						.build());
	}

	/**
	 * Create a single {@link TableQueryV3} covering this {@link TableQueryV4}. It typically leads to unnecessary
	 * computations as it requests a cartesian product between {@link IGroupBy} and {@link FilteredAggregator}.
	 * 
	 * @return a {@link TableQueryV3} covering this V4.
	 */
	public TableQueryV3 asCoveringV3() {
		// Normalize index to 0, deduplicate by (aggregator, filter), then re-index name conflicts.
		// Normalization ensures (aggregator, filter)-equivalent FAs from different groupBys collapse into one
		// even if they carried different per-groupBy indices.
		List<FilteredAggregator> deduped =
				groupByToAggregators.values().stream().map(fa -> fa.withIndex(0)).distinct().toList();

		AtomicLongMap<String> nameToNextIndex = AtomicLongMap.create();
		Set<FilteredAggregator> aggregators = deduped.stream().map(fa -> {
			long i = nameToNextIndex.getAndIncrement(fa.getAggregator().getName());
			return fa.withIndex(i);
		}).collect(ImmutableSet.toImmutableSet());
		Set<IGroupBy> groupBys = ImmutableSet.copyOf(groupByToAggregators.keySet());

		return TableQueryV3.builder()
				.filter(filter)
				.groupBys(groupBys)
				.aggregators(aggregators)
				.customMarker(customMarker)
				.topClause(topClause)
				.options(options)
				.build();
	}

	protected TableQueryStep.TableQueryStepBuilder asQueryStep() {
		return TableQueryStep.builder().customMarker(customMarker).filter(filter).options(options);
	}

	public TableQueryStep recombineQueryStep(IFilterOptimizer filterOptimizer,
			FilteredAggregator filteredAggregator,
			IGroupBy groupBy) {
		// Recombine the stepFilter given the tableQuery filter and the measure filter
		// BEWARE as queryStep is used as key, it is crucial that `AndFilter.and(...)` is equal to the
		// original filter, which may be false in case of some optimization in `AndFilter` (e.g. preferring
		// some `!OR`).
		ISliceFilter recombinedFilter =
				FilterBuilder.and(getFilter(), filteredAggregator.getFilter()).optimize(filterOptimizer);

		return asQueryStep().filter(recombinedFilter)
				.aggregator(filteredAggregator.getAggregator())
				.groupBy(groupBy)
				.build();
	}

	/**
	 * Lombok @Builder
	 */
	public static class TableQueryV4Builder {

		@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
		protected SetMultimap<IGroupBy, FilteredAggregator> groupByToAggregators =
				MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();

		public TableQueryV4Builder groupByToAggregator(IGroupBy groupBy,
				FilteredAggregator aggregator,
				FilteredAggregator... moreAggregators) {
			groupByToAggregators.put(groupBy, aggregator);
			groupByToAggregators.putAll(groupBy, Arrays.asList(moreAggregators));
			return this;
		}

		public TableQueryV4Builder groupByToAggregators(IGroupBy groupBy,
				Collection<? extends FilteredAggregator> aggregators) {
			groupByToAggregators.putAll(groupBy, aggregators);
			return this;
		}

		public TableQueryV4Builder groupByToAggregators(
				SetMultimap<IGroupBy, FilteredAggregator> groupByToAggregators) {
			this.groupByToAggregators.putAll(groupByToAggregators);
			return this;
		}

		/**
		 * Register the cartesian product of aggregators against groupBys.
		 * 
		 * @param groupBys
		 * @param aggregators
		 * @return
		 */
		public TableQueryV4Builder groupByToAggregators(Collection<IGroupBy> groupBys,
				Collection<FilteredAggregator> aggregators) {
			groupBys.forEach(groupBy -> groupByToAggregators(groupBy, aggregators));
			return this;
		}

		public TableQueryV4Builder clearGroupByToAggregators() {
			groupByToAggregators.clear();
			return this;
		}

	}

	public Optional<IGroupBy> singleGroupBy() {
		if (groupByToAggregators.keySet().isEmpty()) {
			return Optional.of(IGroupBy.GRAND_TOTAL);
		} else if (groupByToAggregators.keySet().size() == 1) {
			return groupByToAggregators.keySet().stream().findAny();
		}
		return Optional.empty();
	}

	public Set<FilteredAggregator> getAggregators(IGroupBy groupBy) {
		return groupByToAggregators.get(groupBy);
	}

	public ImmutableSet<FilteredAggregator> getAggregators() {
		return ImmutableSet.copyOf(groupByToAggregators.values());
	}

	@Override
	public Set<IGroupBy> getGroupBys() {
		return getGroupByToAggregators().keySet();
	}

	/**
	 * Returns {@code true} when all {@link IGroupBy}s in this query share exactly the same {@link FilteredAggregator}
	 * set, meaning a single covering {@link TableQueryV3} (GROUPING SET SQL) computes no irrelevant combinations. When
	 * {@code false}, a UNION ALL decomposition (via {@link #streamV3()}) avoids wasteful cartesian-product computation.
	 *
	 * <p>
	 * This flag is the shared decision point between sites that choose the execution strategy (e.g.
	 * {@code JooqTableQueryFactory.prepareQuery}) and sites that describe it in logs or metrics (e.g.
	 * {@code TableQueryEngine.toPerfLog}).
	 */
	public boolean isPerfectV3() {
		return groupByToAggregators.keySet()
				.stream()
				.map(groupBy -> groupByToAggregators.get(groupBy)
						.stream()
						.map(fa -> fa.withIndex(0))
						.collect(ImmutableSet.toImmutableSet()))
				.distinct()
				.count() <= 1;
	}

	@Override
	public Set<String> getGroupedByColumns() {
		return getGroupBys().stream()
				.flatMap(gb -> gb.getGroupedByColumns().stream())
				.distinct()
				.collect(ImmutableSet.toImmutableSet());
	}
}

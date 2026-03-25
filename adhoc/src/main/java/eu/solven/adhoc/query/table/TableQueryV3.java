/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.CubeQueryStep.CubeQueryStepBuilder;
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
 * A query over an {@link ITableWrapper}, which typically represents an external database.
 * 
 * This v3 covers `GROUPING SET` and `FILTER` syntaxes enabling to cover more {@link CubeQueryStep} in a single table
 * query.
 * 
 * @author Benoit Lacelle
 * @see eu.solven.adhoc.table.transcoder.ITableAliaser
 */
@Value
@Builder(toBuilder = true)
// https://blog.jooq.org/how-to-calculate-multiple-aggregate-functions-in-a-single-query/
public class TableQueryV3 implements ITableQuery {

	// a filter shared through all aggregators
	@Default
	ISliceFilter filter = ISliceFilter.MATCH_ALL;

	// We may have multiple `GROUP BY` through a `GROUPING SET` clause
	@Singular
	@NonNull
	ImmutableSet<IGroupBy> groupBys;

	// SQL enable `FILTER` clause on a per-aggregation basis
	@Singular
	@NonNull
	ImmutableSet<FilteredAggregator> aggregators;

	// This property is transported down to the TableQuery
	@Default
	Object customMarker = null;

	@Default
	AdhocTopClause topClause = AdhocTopClause.NO_LIMIT;

	@NonNull
	@Singular
	ImmutableSet<IQueryOption> options;

	// TODO If this method is actual relevant, it should return an IGroupBy
	public Map<String, IAdhocColumn> getColumns() {
		// https://stackoverflow.com/questions/23699371/distinct-by-property
		return getGroupBys().stream()
				.flatMap(gb -> gb.getColumns().stream())
				.collect(Collectors.toMap(IAdhocColumn::getName, p -> p, (p, q) -> {
					if (!Objects.equals(p, q)) {
						throw new IllegalArgumentException("%s!=%s".formatted(p, q));
					}
					return p;
				}, LinkedHashMap::new));
	}

	public static TableQueryV3.TableQueryV3Builder edit(TableQuery tableQuery) {
		return edit(TableQueryV2.edit(tableQuery).build());
	}

	public static TableQueryV3.TableQueryV3Builder edit(TableQueryV2 tableQuery) {
		// Like V2, but with a single groupBy
		return TableQueryV3.builder()
				.groupBy(tableQuery.getGroupBy())
				.filter(tableQuery.getFilter())
				.customMarker(tableQuery.getCustomMarker())
				.options(tableQuery.getOptions())
				.topClause(tableQuery.getTopClause())
				.aggregators(tableQuery.getAggregators());
	}

	public Stream<CubeQueryStep> cubeQuerySteps(IFilterOptimizer filterOptimizer) {
		return getAggregators().stream()
				.flatMap(a -> getGroupBys().stream().map(gb -> recombineQueryStep(filterOptimizer, a, gb)));
	}

	protected CubeQueryStepBuilder asQueryStep() {
		return CubeQueryStep.builder().customMarker(customMarker).filter(filter).options(options);
	}

	@Deprecated(since = "V4", forRemoval = true)
	protected CubeQueryStep recombineQueryStep(IFilterOptimizer filterOptimizer,
			FilteredAggregator filteredAggregator,
			IGroupBy groupBy) {
		// Recombine the stepFilter given the tableQuery filter and the measure filter
		// BEWARE as queryStep is used as key, it is crucial that `AndFilter.and(...)` is equal to the
		// original filter, which may be false in case of some optimization in `AndFilter` (e.g. preferring
		// some `!OR`).
		ISliceFilter recombinedFilter =
				FilterBuilder.and(getFilter(), filteredAggregator.getFilter()).optimize(filterOptimizer);

		return asQueryStep().filter(recombinedFilter)
				.measure(filteredAggregator.getAggregator())
				.groupBy(groupBy)
				.build();
	}

	public Stream<IGroupBy> streamGroupBy() {
		Stream<IGroupBy> groupBysStream;

		if (groupBys.isEmpty()) {
			groupBysStream = Stream.of(IGroupBy.GRAND_TOTAL);
		} else {
			groupBysStream = groupBys.stream();
		}
		return groupBysStream;
	}

	public static long nbCuboids(TableQueryV3 v3) {
		return v3.getAggregators().size() * v3.streamGroupBy().count();
	}

	public Stream<TableQueryV2> streamV2() {
		Stream<IGroupBy> groupBysStream = streamGroupBy();
		return groupBysStream.map(groupBy -> TableQueryV2.builder()
				.aggregators(getAggregators())
				.customMarker(getCustomMarker())
				.filter(getFilter())
				.groupBy(groupBy)
				.options(getOptions())
				.topClause(getTopClause())
				.build());
	}

	public Optional<IGroupBy> singleGroupBy() {
		if (groupBys.isEmpty()) {
			return Optional.of(IGroupBy.GRAND_TOTAL);
		} else if (groupBys.size() == 1) {
			return groupBys.stream().findFirst();
		} else {
			return Optional.empty();
		}
	}

	@Override
	public Set<String> getGroupedByColumns() {
		return getGroupBys().stream()
				.flatMap(gb -> gb.getColumns().stream().map(IAdhocColumn::getName))
				.collect(ImmutableSet.toImmutableSet());
	}

	public static TableQueryV3Builder edit(CubeQueryStep step) {
		return TableQueryV3.builder().options(step.getOptions()).customMarker(step.getCustomMarker());
	}

	public TableQueryV4 toV4() {
		return TableQueryV4.edit(this).build();
	}

}
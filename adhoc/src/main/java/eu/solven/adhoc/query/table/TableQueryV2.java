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

import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.options.IQueryOption;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.cube.IHasCustomMarker;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
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
 * It is similar to {@link TableQuery} but enables {@link Aggregator} to be filtered, hence enabling covering more
 * {@link CubeQueryStep}.
 * 
 * @author Benoit Lacelle
 * @see eu.solven.adhoc.table.transcoder.ITableAliaser
 */
@Value
@Builder(toBuilder = true)
// https://blog.jooq.org/how-to-calculate-multiple-aggregate-functions-in-a-single-query/
public class TableQueryV2 implements IWhereGroupByQuery, IHasCustomMarker, IHasQueryOptions {

	// a filter shared through all aggregators
	@Default
	ISliceFilter filter = ISliceFilter.MATCH_ALL;

	@Default
	IGroupBy groupBy = IGroupBy.GRAND_TOTAL;

	// We query only simple aggregations to external databases
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

	public static TableQueryV2.TableQueryV2Builder edit(TableQuery tableQuery) {
		return TableQueryV2.builder()
				.groupBy(tableQuery.getGroupBy())
				.filter(tableQuery.getFilter())
				.aggregators(tableQuery.getAggregators()
						.stream()
						.map(a -> FilteredAggregator.builder().aggregator(a).build())
						.toList())
				.customMarker(tableQuery.getCustomMarker())
				.options(tableQuery.getOptions())
				.topClause(tableQuery.getTopClause());
	}

	public TableQueryV3 toV3() {
		return TableQueryV3.edit(this).build();
	}

	// TODO Refactor with eu.solven.adhoc.query.table.TableQueryV3.recombineQueryStep(IFilterOptimizer,
	// FilteredAggregator, IGroupBy)
	public Stream<TableQuery> toV1(IFilterOptimizer filterOptimizer) {
		return aggregators.stream()
				.map(fa -> TableQuery.edit(this)
						.aggregator(fa.getAggregator())
						.filter(FilterBuilder.and(getFilter(), fa.getFilter()).optimize(filterOptimizer))
						.build());
	}
}
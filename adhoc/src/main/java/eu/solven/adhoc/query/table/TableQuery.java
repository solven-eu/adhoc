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

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.IHasMeasures;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.options.IQueryOption;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.cube.IHasCustomMarker;
import eu.solven.adhoc.query.cube.IHasMeasure;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.ISliceFilter;
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
 * This relates to {@link CubeQueryStep} by enabling multiple {@link Aggregator}.
 * 
 * @author Benoit Lacelle
 * @see eu.solven.adhoc.table.transcoder.ITableAliaser
 */
@Value
@Builder(toBuilder = true)
public class TableQuery implements IWhereGroupByQuery, IHasCustomMarker, IHasQueryOptions {

	@Default
	ISliceFilter filter = ISliceFilter.MATCH_ALL;

	@Default
	IGroupBy groupBy = IGroupBy.GRAND_TOTAL;

	// We query only simple aggregations to external databases
	@Singular
	@NonNull
	ImmutableSet<Aggregator> aggregators;

	// This property is transported down to the TableQuery
	@Default
	Object customMarker = null;

	@Default
	AdhocTopClause topClause = AdhocTopClause.NO_LIMIT;

	@NonNull
	@Singular
	ImmutableSet<IQueryOption> options;

	/**
	 * Never empty. If built on an empty {@link Set}, return the empty {@link Aggregator}.
	 * 
	 * @return
	 */
	public Set<Aggregator> getAggregators() {
		if (aggregators.isEmpty()) {
			return ImmutableSet.of(Aggregator.empty());
		}
		return aggregators;
	}

	@Deprecated(since = "use .toBuilder()", forRemoval = true)
	public static TableQueryBuilder edit(TableQuery tableQuery) {
		return tableQuery.toBuilder();
	}

	public static TableQueryBuilder edit(IWhereGroupByQuery query) {
		TableQueryBuilder builder = TableQuery.builder().filter(query.getFilter()).groupBy(query.getGroupBy());

		if (query instanceof IHasCustomMarker hasCustomMarker) {
			hasCustomMarker.optCustomMarker().ifPresent(builder::customMarker);
		}
		if (query instanceof IHasQueryOptions hasQueryOptions) {
			builder.options(hasQueryOptions.getOptions());
		}
		if (query instanceof IHasMeasure hasMeasure) {
			IMeasure measure = hasMeasure.getMeasure();
			if (measure instanceof Aggregator aggregator) {
				builder.aggregator(aggregator);
			}
		}
		if (query instanceof IHasMeasures hasMeasures) {
			Set<IMeasure> measures = hasMeasures.getMeasures();

			measures.forEach(measure -> {
				if (measure instanceof Aggregator aggregator) {
					builder.aggregator(aggregator);
				}
			});
		}

		return builder;
	}

	/**
	 * 
	 * @param aggregatorSteps
	 *            {@link CubeQueryStep}, each associated to an {@link Aggregator}
	 * @return an equivalent {@link Set} of {@link TableQuery}
	 */
	public static Set<TableQuery> fromSteps(Set<CubeQueryStep> aggregatorSteps) {
		return aggregatorSteps.stream()
				.map(step -> edit(step).aggregator((Aggregator) step.getMeasure()).build())
				.collect(ImmutableSet.toImmutableSet());
	}
}

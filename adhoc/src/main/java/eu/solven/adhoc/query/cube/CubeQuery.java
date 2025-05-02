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
package eu.solven.adhoc.query.cube;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.debug.IIsDebugable;
import eu.solven.adhoc.debug.IIsExplainable;
import eu.solven.adhoc.measure.IHasMeasures;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.util.NotYetImplementedException;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Simple {@link ICubeQuery}, where the filter is an AND condition.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
@Jacksonized
public class CubeQuery implements ICubeQuery, IHasCustomMarker, IHasQueryOptions {

	@NonNull
	@Default
	IAdhocFilter filter = IAdhocFilter.MATCH_ALL;
	@NonNull
	@Default
	IAdhocGroupBy groupBy = IAdhocGroupBy.GRAND_TOTAL;
	// @Singular
	ImmutableSet<IMeasure> measures;

	// This property is transported down to the DatabaseQuery
	// Not an Optional as JDK consider Optional are good only as return value
	@Default
	Object customMarker = null;

	@NonNull
	@Singular
	ImmutableSet<IQueryOption> options;

	@Override
	@JsonIgnore
	public boolean isDebug() {
		return options.contains(StandardQueryOptions.DEBUG);
	}

	@Override
	@JsonIgnore
	public boolean isExplain() {
		return options.contains(StandardQueryOptions.EXPLAIN);
	}

	@Override
	public Optional<?> optCustomMarker() {
		return Optional.ofNullable(customMarker);
	}

	@Override
	public Set<IMeasure> getMeasures() {
		return measures;
	}

	public static class CubeQueryBuilder {
		// @Singular
		ImmutableSet<IMeasure> measures = ImmutableSet.of();

		// https://github.com/projectlombok/lombok/pull/3193
		public CubeQueryBuilder measure(String firstName, String... moreNames) {
			Lists.asList(firstName, moreNames).forEach(measureName -> {
				this.measure(ReferencedMeasure.ref(measureName));
			});

			return this;
		}

		/**
		 * 
		 * Append measures to the query.
		 * 
		 * @param measureNames
		 *            referencing measures in the {@link IMeasureForest}
		 * @return
		 */
		public CubeQueryBuilder measureNames(Collection<String> measureNames) {
			measureNames.stream().map(ReferencedMeasure::ref).forEach(this::measure);

			return this;
		}

		/**
		 * Append measures to the query.
		 * 
		 * BEWARE Even if we accept {@link IMeasure}, these measures are expected to be registered in the measure bag.
		 * This may be lifted in a later version.
		 *
		 * @param first
		 * @param more
		 * @return
		 */
		public CubeQueryBuilder measure(IMeasure first, IMeasure... more) {
			return measures(Lists.asList(first, more));
		}

		/**
		 * Append measures to the query.
		 * 
		 * BEWARE Even if we accept {@link IMeasure}, these measures are expected to be registered in the measure bag.
		 * This may be lifted in a later version.
		 * 
		 * @param measures
		 * @return
		 */
		public CubeQueryBuilder measures(Collection<? extends IMeasure> measures) {
			this.measures =
					Stream.concat(this.measures.stream(), measures.stream()).collect(ImmutableSet.toImmutableSet());

			return this;
		}

		/**
		 * `AND` existing {@link IAdhocFilter} with an {@link IColumnFilter}
		 *
		 * @param filter
		 * @return the builder
		 */
		public CubeQueryBuilder andFilter(IAdhocFilter filter) {
			filter(AndFilter.and(build().getFilter(), filter));

			return this;
		}

		/**
		 * `AND` existing {@link IAdhocFilter} with an {@link IColumnFilter}
		 *
		 * @param column
		 * @param value
		 * @return the builder
		 */
		public CubeQueryBuilder andFilter(String column, Object value) {
			return andFilter(ColumnFilter.builder().column(column).matching(value).build());
		}

		public CubeQueryBuilder groupByAlso(Collection<? extends IAdhocColumn> groupBys) {
			Set<IAdhocColumn> allGroupByColumns = new HashSet<>();

			// https://stackoverflow.com/questions/66260030/get-value-of-field-with-lombok-builder
			allGroupByColumns.addAll(this.build().getGroupBy().getNameToColumn().values());
			groupBys.stream().forEach(allGroupByColumns::add);

			groupBy(GroupByColumns.of(allGroupByColumns));

			return this;
		}

		public CubeQueryBuilder groupByAlso(IAdhocColumn firstGroupBy, IAdhocColumn... moreGroupBys) {
			return groupByAlso(Lists.asList(firstGroupBy, moreGroupBys));
		}

		public CubeQueryBuilder groupByAlso(String firstGroupBy, String... moreGroupBys) {
			groupByAlso(Lists.asList(firstGroupBy, moreGroupBys).stream().map(ReferencedColumn::ref).toList());

			return this;
		}

		public CubeQueryBuilder customMarker(Object custom) {
			if (custom instanceof Optional<?> optional) {
				// Custom variable is either a not-Optional or a null
				// `optCustomMarker` would wrap in an Optional
				custom = optional.orElse(null);
			}

			this.customMarker$value = custom;
			this.customMarker$set = true;

			return this;
		}

		public CubeQueryBuilder debug(boolean isDebug) {
			if (isDebug) {
				return this.option(StandardQueryOptions.DEBUG);
			} else {
				// It should be rare to remove debug
				throw new NotYetImplementedException("TODO");
			}
		}

		public CubeQueryBuilder explain(boolean isExplain) {
			if (isExplain) {
				return this.option(StandardQueryOptions.EXPLAIN);
			} else {
				// It should be rare to remove explain
				throw new NotYetImplementedException("TODO");
			}
		}
	}

	/**
	 * BEWARE This may lose additional information not fitting into an {@link CubeQuery}, like the {@link AdhocQueryId}
	 * of a {@link AdhocSubQuery}.
	 * 
	 * @param query
	 * @return
	 */
	public static CubeQueryBuilder edit(IWhereGroupByQuery query) {
		CubeQueryBuilder builder = CubeQuery.builder().filter(query.getFilter()).groupBy(query.getGroupBy());

		if (query instanceof IHasMeasures hasMeasures) {
			builder.measures(hasMeasures.getMeasures());
		}
		if (query instanceof IIsExplainable isExplainable && isExplainable.isExplain()) {
			builder.option(StandardQueryOptions.EXPLAIN);
		}
		if (query instanceof IIsDebugable isDebugable && isDebugable.isDebug()) {
			builder.option(StandardQueryOptions.DEBUG);
		}
		if (query instanceof IHasCustomMarker hasCustomMarker) {
			builder.customMarker(hasCustomMarker.getCustomMarker());
		}
		if (query instanceof IHasQueryOptions hasQueryOptions) {
			builder.options(hasQueryOptions.getOptions());
		}

		return builder;
	}
}
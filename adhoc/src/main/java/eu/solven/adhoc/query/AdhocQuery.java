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
package eu.solven.adhoc.query;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Lists;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.api.v1.IHasCustomMarker;
import eu.solven.adhoc.api.v1.pojo.AndFilter;
import eu.solven.adhoc.api.v1.pojo.ColumnFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.groupby.IAdhocColumn;
import eu.solven.adhoc.query.groupby.ReferencedColumn;
import eu.solven.adhoc.transformers.ReferencedMeasure;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Simple {@link IAdhocQuery}, where the filter is an AND condition.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
@Jacksonized
public class AdhocQuery implements IAdhocQuery, IHasCustomMarker {

	@NonNull
	@Default
	IAdhocFilter filter = IAdhocFilter.MATCH_ALL;
	@NonNull
	@Default
	IAdhocGroupBy groupBy = IAdhocGroupBy.GRAND_TOTAL;
	@Singular
	Set<ReferencedMeasure> measureRefs;

	// This property is transported down to the DatabaseQuery
	// Not an Optional as JDK consider Optional are good only as return value
	@Default
	Object customMarker = null;

	// If true, will print a log of debug information
	@Default
	boolean debug = false;
	// If true, will print details about the query plan
	@Default
	boolean explain = false;

	public Optional<?> optCustomMaker() {
		return Optional.ofNullable(customMarker);
	}

	@Override
	public Set<ReferencedMeasure> getMeasureRefs() {
		return measureRefs;
	}

	public static class AdhocQueryBuilder {
		public AdhocQueryBuilder measure(String firstName, String... moreNames) {
			Lists.asList(firstName, moreNames).forEach(measureName -> {
				this.measureRef(ReferencedMeasure.ref(measureName));
			});

			return this;
		}

		public AdhocQueryBuilder measures(Collection<String> measureNames) {
			measureNames.stream().map(ReferencedMeasure::ref).forEach(this::measureRef);

			return this;
		}

		/**
		 * `AND` existing {@link IAdhocFilter} with an {@link eu.solven.adhoc.api.v1.filters.IColumnFilter}
		 *
		 * @param filter
		 * @return the builder
		 */
		public AdhocQueryBuilder andFilter(IAdhocFilter filter) {
			filter(AndFilter.and(build().getFilter(), filter));

			return this;
		}

		/**
		 * `AND` existing {@link IAdhocFilter} with an {@link eu.solven.adhoc.api.v1.filters.IColumnFilter}
		 *
		 * @param column
		 * @param value
		 * @return the builder
		 */
		public AdhocQueryBuilder andFilter(String column, Object value) {
			return andFilter(ColumnFilter.builder().column(column).matching(value).build());
		}

		public AdhocQueryBuilder groupByAlso(String firstGroupBy, String... moreGroupBys) {
			Set<IAdhocColumn> allGroupByColumns = new HashSet<>();

			// https://stackoverflow.com/questions/66260030/get-value-of-field-with-lombok-builder
			allGroupByColumns.addAll(this.build().getGroupBy().getNameToColumn().values());
			Lists.asList(firstGroupBy, moreGroupBys)
					.stream()
					.map(c -> ReferencedColumn.ref(c))
					.forEach(allGroupByColumns::add);

			groupBy(GroupByColumns.of(allGroupByColumns));

			return this;
		}
	}

	public AdhocQueryBuilder edit(AdhocQuery query) {
		return AdhocQuery.builder()
				.debug(query.isDebug())
				.explain(query.isExplain())
				.measureRefs(query.getMeasureRefs())
				.filter(query.getFilter())
				.groupBy(query.getGroupBy());
	}
}
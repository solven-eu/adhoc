package eu.solven.adhoc.query;

import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.Lists;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.api.v1.pojo.AndFilter;
import eu.solven.adhoc.api.v1.pojo.ColumnFilter;
import eu.solven.adhoc.transformers.ReferencedMeasure;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

/**
 * Simple {@link IAdhocQuery}, where the filter is an AND condition.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
public class AdhocQuery implements IAdhocQuery {

	@NonNull
	@Default
	IAdhocFilter filter = IAdhocFilter.MATCH_ALL;
	@NonNull
	@Default
	IAdhocGroupBy groupBy = IAdhocGroupBy.GRAND_TOTAL;
	@Singular
	Set<ReferencedMeasure> measureRefs;

	// If true, will print a log of debug information
	@Default
	boolean debug = false;
	// If true, will print details about the query plan
	@Default
	boolean explain = false;

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

		public AdhocQueryBuilder andFilter(IAdhocFilter moreAndFilter) {
			filter(AndFilter.and(build().getFilter(), moreAndFilter));

			return this;
		}

		public AdhocQueryBuilder andFilter(String key, Object value) {
			return andFilter(new ColumnFilter(key, value));
		}

		public AdhocQueryBuilder groupByColumns(String firstGroupBy, String... moreGroupBys) {
			Set<String> allGroupByColumns = new TreeSet<>();

			// https://stackoverflow.com/questions/66260030/get-value-of-field-with-lombok-builder
			allGroupByColumns.addAll(this.build().getGroupBy().getGroupedByColumns());
			allGroupByColumns.addAll(Lists.asList(firstGroupBy, moreGroupBys));

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
package eu.solven.adhoc.query;

import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.api.v1.pojo.AndFilter;
import eu.solven.adhoc.api.v1.pojo.EqualsFilter;
import eu.solven.adhoc.transformers.ReferencedMeasure;

/**
 * Helps building {@link AdhocQuery}.
 * 
 * @author Benoit Lacelle
 *
 */
public class AdhocQueryBuilder {
	// By default, no columns
	/**
	 * The columns along which the result has to be expanded.
	 */
	protected final Set<String> wildcards = new ConcurrentSkipListSet<>();

	// By default, no filters
	protected final List<IAdhocFilter> andFilters = new CopyOnWriteArrayList<>();

	// By default, no aggregation
	protected final NavigableSet<ReferencedMeasure> measures = new ConcurrentSkipListSet<>();

	boolean debug = false;
	boolean explain = false;

	public AdhocQueryBuilder addGroupby(String wildcard, String... more) {
		return addWildcards(Lists.asList(wildcard, more));
	}

	public AdhocQueryBuilder addWildcards(Iterable<String> moreWildcards) {
		Iterables.addAll(this.wildcards, moreWildcards);

		return this;
	}

	public AdhocQueryBuilder andFilter(IAdhocFilter moreAndFilter) {
		// Skipping matchAll is useful on `.edit`
		if (!moreAndFilter.isMatchAll()) {
			andFilters.add(moreAndFilter);
		}

		return this;
	}

	public AdhocQueryBuilder addFilter(String key, Object value) {
		return andFilter(new EqualsFilter(key, value));
	}

	public AdhocQueryBuilder addMeasures(Iterable<ReferencedMeasure> moreMeasures) {
		Iterables.addAll(this.measures, moreMeasures);

		return this;
	}

	public AdhocQueryBuilder addMeasures(ReferencedMeasure aggregatedAxis, ReferencedMeasure... more) {
		return addMeasures(Lists.asList(aggregatedAxis, more));
	}

	public AdhocQueryBuilder addMeasures(String measure, String... moreMeasures) {
		List<String> measureNames = Lists.asList(measure, moreMeasures);
		return addMeasures(measureNames);
	}

	public AdhocQueryBuilder addMeasures(List<String> measureNames) {
		List<ReferencedMeasure> asRefs = measureNames.stream()
				.map(name -> ReferencedMeasure.builder().ref(name).build())
				.collect(Collectors.toList());

		return addMeasures(asRefs);
	}

	public AdhocQueryBuilder clearMeasures() {
		measures.clear();

		return this;
	}

	public AdhocQueryBuilder debug(boolean debug) {
		this.debug = debug;
		return this;
	}

	public AdhocQueryBuilder explain(boolean explain) {
		this.explain = explain;
		return this;
	}

	public AdhocQuery build() {
		IAdhocFilter filter;
		if (andFilters.isEmpty()) {
			filter = IAdhocFilter.MATCH_ALL;
		} else if (andFilters.size() == 1) {
			filter = andFilters.get(0);
		} else {
			filter = new AndFilter(andFilters);
		}

		IAdhocGroupBy groupBy;
		if (wildcards.isEmpty()) {
			groupBy = IAdhocGroupBy.GRAND_TOTAL;
		} else {
			groupBy = GroupByColumns.of(wildcards);
		}

		AdhocQuery query = AdhocQuery.builder()
				.filter(filter)
				.groupBy(groupBy)
				.measures(measures)
				.debug(debug)
				.explain(explain)
				.build();

		return query;
	}

	public static AdhocQueryBuilder grandTotal() {
		return new AdhocQueryBuilder();
	}

	public static AdhocQueryBuilder filter(String key, String value) {
		AdhocQueryBuilder queryBuilder = new AdhocQueryBuilder();

		queryBuilder.addFilter(key, value);

		return queryBuilder;
	}

	public static AdhocQueryBuilder wildcards(String wildcard, String... more) {
		return wildcards(Lists.asList(wildcard, more));
	}

	public static AdhocQueryBuilder wildcards(Iterable<String> wildcards) {
		AdhocQueryBuilder queryBuilder = new AdhocQueryBuilder();

		return queryBuilder.addWildcards(wildcards);
	}

	public static AdhocQueryBuilder measure(String measure, String... moreMeasures) {
		AdhocQueryBuilder queryBuilder = new AdhocQueryBuilder();

		queryBuilder.addMeasures(measure, moreMeasures);

		return queryBuilder;
	}

	/**
	 * Pre-fill a builder given an instance.
	 * 
	 * @param base
	 * @return
	 */
	public static AdhocQueryBuilder edit(IAdhocQuery base) {
		AdhocQueryBuilder queryBuilder = new AdhocQueryBuilder();

		for (ReferencedMeasure wildcard : base.getMeasures()) {
			queryBuilder.addMeasures(wildcard);
		}

		queryBuilder.andFilter(base.getFilter());

		for (String wildcard : base.getGroupBy().getGroupedByColumns()) {
			queryBuilder.addGroupby(wildcard);
		}

		if (base instanceof AdhocQuery adhocQuery) {
			queryBuilder.debug = adhocQuery.isDebug();
			queryBuilder.explain = adhocQuery.isExplain();
		}

		return queryBuilder;
	}

}
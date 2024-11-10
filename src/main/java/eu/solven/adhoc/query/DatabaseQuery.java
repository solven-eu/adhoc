package eu.solven.adhoc.query;

import java.util.Collections;
import java.util.Set;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.api.v1.IHasFilters;
import eu.solven.adhoc.api.v1.IHasGroupBy;
import eu.solven.adhoc.api.v1.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.transformers.Aggregator;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

/**
 * A Database query is dedicated to querying external database.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@AllArgsConstructor
@Builder
public class DatabaseQuery implements IWhereGroupbyAdhocQuery {

	@Default
	protected final IAdhocFilter filter = IAdhocFilter.MATCH_ALL;

	@Default
	protected final IAdhocGroupBy groupBy = IAdhocGroupBy.GRAND_TOTAL;
	// We query only simple aggregations to external databases
	@Default
	protected final Set<Aggregator> aggregators = Collections.emptySet();

	public DatabaseQuery(IHasFilters hasFilter, IHasGroupBy groupBy, Set<Aggregator> aggregators) {
		this.filter = hasFilter.getFilter();
		this.groupBy = groupBy.getGroupBy();
		this.aggregators = aggregators;
	}

	@Override
	public IAdhocFilter getFilter() {
		return filter;
	}

	@Override
	public IAdhocGroupBy getGroupBy() {
		return groupBy;
	}

	public Set<Aggregator> getAggregators() {
		return aggregators;
	}

	// @Override
	// public String toString() {
	// // We call the getters to workaround usage of lambda
	// return "SimpleAggregationQuery [axesFilters=" + axesFilters
	// .getFilter() + ", groupBys=" + axes.getGroupBys() + ", aggregators=" + aggregators + "]";
	// }
}
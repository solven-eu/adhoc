package eu.solven.adhoc.query;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import eu.solven.adhoc.api.v1.IAxesFilter;
import eu.solven.adhoc.api.v1.IHasFilters;
import eu.solven.adhoc.api.v1.IHasGroupBys;
import eu.solven.adhoc.api.v1.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.api.v1.pojo.AxesFilterAnd;
import eu.solven.adhoc.transformers.Aggregator;
import lombok.Builder;
import lombok.Builder.Default;

/**
 * A Database query is dedicated to querying external database.
 * 
 * @author Benoit Lacelle
 *
 */
@Builder
public class DatabaseQuery implements IWhereGroupbyAdhocQuery {

	@Default
	protected final IHasFilters axesFilters = () -> AxesFilterAnd.MATCH_ALL;

	@Default
	protected final IHasGroupBys axes = () -> Collections.emptyList();
	// We query only simple aggregations to external databases
	@Default
	protected final Set<Aggregator> aggregators = Collections.emptySet();

	public DatabaseQuery(IHasFilters hasFilters, IHasGroupBys axes, Set<Aggregator> aggregators) {
		this.axesFilters = hasFilters;
		this.axes = axes;
		this.aggregators = aggregators;
	}

	@Override
	public IAxesFilter getFilters() {
		return axesFilters.getFilters();
	}

	@Override
	public List<String> getGroupBys() {
		return axes.getGroupBys();
	}

	public Set<Aggregator> getAggregators() {
		return aggregators;
	}

	@Override
	public String toString() {
		// We call the getters to workaround usage of lambda
		return "SimpleAggregationQuery [axesFilters=" + axesFilters
				.getFilters() + ", groupBys=" + axes.getGroupBys() + ", aggregators=" + aggregators + "]";
	}
}
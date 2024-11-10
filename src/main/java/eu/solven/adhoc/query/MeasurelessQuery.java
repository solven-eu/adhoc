package eu.solven.adhoc.query;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.api.v1.IWhereGroupbyAdhocQuery;
import lombok.Builder;
import lombok.Value;

/**
 * Simple {@link IAdhocQuery}, where the filter is an AND condition.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
public class MeasurelessQuery implements IWhereGroupbyAdhocQuery {

	IAdhocFilter filter;
	IAdhocGroupBy groupBy;

	public static MeasurelessQuery of(IWhereGroupbyAdhocQuery q) {
		return MeasurelessQuery.builder().filter(q.getFilter()).groupBy(q.getGroupBy()).build();
	}
}
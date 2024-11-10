package eu.solven.adhoc.query;

import java.util.Set;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.transformers.ReferencedMeasure;
import lombok.Value;

/**
 * Simple {@link IAdhocQuery}, where the filter is an AND condition.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
public class AdhocQuery implements IAdhocQuery {

	protected final IAdhocFilter filter;
	protected final IAdhocGroupBy groupBy;
	protected final Set<ReferencedMeasure> measures;

}
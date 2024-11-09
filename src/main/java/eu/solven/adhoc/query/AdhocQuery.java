package eu.solven.adhoc.query;

import java.util.List;
import java.util.Set;

import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.api.v1.IAxesFilter;
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

	protected final IAxesFilter filters;
	protected final List<String> groupBys;
	protected final Set<ReferencedMeasure> measures;

}
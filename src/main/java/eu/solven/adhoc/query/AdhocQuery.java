package eu.solven.adhoc.query;

import java.util.Set;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.transformers.ReferencedMeasure;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
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
	IAdhocFilter filter;
	@NonNull
	IAdhocGroupBy groupBy;
	@NonNull
	Set<ReferencedMeasure> measures;

	// If true, will print a log of debug information
	@Default
	boolean debug = false;
	// If true, will print details about the query plan
	@Default
	boolean explain = false;

}
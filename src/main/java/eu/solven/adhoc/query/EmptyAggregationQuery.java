package eu.solven.adhoc.query;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.api.v1.IDynamicAdhocQuery;
import eu.solven.adhoc.api.v1.IHasRefMeasures;
import eu.solven.adhoc.transformers.ReferencedMeasure;

/**
 * Simple {@link IAdhocQuery}, where the filter is an AND condition.
 * 
 * @author Benoit Lacelle
 *
 */
public class EmptyAggregationQuery implements IDynamicAdhocQuery {

	@Override
	public IAdhocFilter getFilter() {
		return IAdhocFilter.MATCH_ALL;
	}

	@Override
	public IAdhocGroupBy getGroupBy() {
		return IAdhocGroupBy.GRAND_TOTAL;
	}

	@Override
	public Set<ReferencedMeasure> getMeasures() {
		return Collections.emptySet();
	}

	@Override
	public IDynamicAdhocQuery addAggregations(IHasRefMeasures additionalAggregations) {
		IHasRefMeasures mergedAggregations = () -> ImmutableSet.<ReferencedMeasure>builder()
				.addAll(getMeasures())
				.addAll(additionalAggregations.getMeasures())
				.build();

		return new AdhocDynamicQuery(this, this, mergedAggregations);
	}
}
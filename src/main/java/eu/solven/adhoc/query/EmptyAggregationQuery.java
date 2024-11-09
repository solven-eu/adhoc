package eu.solven.adhoc.query;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.api.v1.IAxesFilter;
import eu.solven.adhoc.api.v1.IDynamicAdhocQuery;
import eu.solven.adhoc.api.v1.IHasRefMeasures;
import eu.solven.adhoc.api.v1.pojo.AxesFilterAnd;
import eu.solven.adhoc.transformers.ReferencedMeasure;

/**
 * Simple {@link IAdhocQuery}, where the filter is an AND condition.
 * 
 * @author Benoit Lacelle
 *
 */
public class EmptyAggregationQuery implements IDynamicAdhocQuery {

	@Override
	public IAxesFilter getFilters() {
		return new AxesFilterAnd(Collections.emptyList());
	}

	@Override
	public List<String> getGroupBys() {
		return Collections.emptyList();
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
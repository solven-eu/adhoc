package eu.solven.adhoc.query;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.api.v1.IDynamicAdhocQuery;
import eu.solven.adhoc.api.v1.IHasFilters;
import eu.solven.adhoc.api.v1.IHasGroupBy;
import eu.solven.adhoc.api.v1.IHasRefMeasures;
import eu.solven.adhoc.transformers.ReferencedMeasure;
import lombok.EqualsAndHashCode;

/**
 * Simple {@link IAdhocQuery}, where the filter is an AND condition.
 * 
 * @author Benoit Lacelle
 *
 */
@Deprecated(since = "Unclear use of Lambda for properties")
@EqualsAndHashCode
public class AdhocDynamicQuery implements IDynamicAdhocQuery {

	protected final IHasFilters axesFilters;
	protected final IHasGroupBy axes;
	protected final IHasRefMeasures hasMeasures;

	public AdhocDynamicQuery(IHasFilters hasFilters, IHasGroupBy axes, IHasRefMeasures hasMeasures) {
		this.axesFilters = hasFilters;
		this.axes = axes;
		this.hasMeasures = hasMeasures;
	}

	@Override
	public IAdhocFilter getFilter() {
		return axesFilters.getFilter();
	}

	@Override
	public IAdhocGroupBy getGroupBy() {
		return axes.getGroupBy();
	}

	@Override
	public Set<ReferencedMeasure> getMeasures() {
		return hasMeasures.getMeasures();
	}

	@Override
	public String toString() {
		// We call the getters to workaround usage of lambda
		return "SimpleAggregationQuery [axesFilters=" + axesFilters
				.getFilter() + ", groupBys=" + axes.getGroupBy() + ", hasMeasures=" + hasMeasures.getMeasures() + "]";
	}

	@Override
	public IDynamicAdhocQuery addAggregations(IHasRefMeasures additionalMeasures) {
		IHasRefMeasures mergedMeasures = () -> ImmutableSet.<ReferencedMeasure>builder()
				.addAll(getMeasures())
				.addAll(additionalMeasures.getMeasures())
				.build();

		return new AdhocDynamicQuery(this, this, mergedMeasures);
	}
}
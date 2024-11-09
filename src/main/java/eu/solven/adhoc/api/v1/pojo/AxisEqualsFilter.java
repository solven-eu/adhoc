package eu.solven.adhoc.api.v1.pojo;

import java.util.Set;

import eu.solven.adhoc.api.v1.IAxesFilter;
import eu.solven.adhoc.api.v1.filters.IAxesFilterAxisEquals;
import eu.solven.adhoc.query.ICountMeasuresConstants;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
public class AxisEqualsFilter implements IAxesFilterAxisEquals {

	final String axis;
	final Object filtered;

	public AxisEqualsFilter(String axis, Object filtered) {
		if (Set.of(ICountMeasuresConstants.STAR).contains(axis)) {
			throw new IllegalArgumentException("Invalid axis for filter: " + axis);
		}

		this.axis = axis;
		this.filtered = filtered;

		if (filtered == null) {
			throw new IllegalArgumentException("'filtered' can not be null");
		} else if (filtered instanceof IAxesFilter) {
			throw new IllegalArgumentException("'filtered' can not be a: " + IAxesFilter.class.getSimpleName());
		}
	}

	@Override
	public boolean isExclusion() {
		return false;
	}

	@Override
	public boolean isAxisEquals() {
		return true;
	}

}
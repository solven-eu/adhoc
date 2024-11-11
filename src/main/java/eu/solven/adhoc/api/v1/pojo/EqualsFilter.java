package eu.solven.adhoc.api.v1.pojo;

import java.util.Set;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.filters.IEqualsFilter;
import eu.solven.adhoc.query.ICountMeasuresConstants;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
public class EqualsFilter implements IEqualsFilter {

	@NonNull
	final String axis;
	@NonNull
	final Object filtered;

	public EqualsFilter(String axis, Object filtered) {
		if (Set.of(ICountMeasuresConstants.STAR).contains(axis)) {
			throw new IllegalArgumentException("Invalid axis for filter: " + axis);
		}

		this.axis = axis;
		this.filtered = filtered;

		if (filtered == null) {
			throw new IllegalArgumentException("'filtered' can not be null");
		} else if (filtered instanceof IAdhocFilter) {
			throw new IllegalArgumentException("'filtered' can not be a: " + IAdhocFilter.class.getSimpleName());
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
package eu.solven.adhoc.api.v1.pojo;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.filters.IColumnFilter;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
public class ColumnFilter implements IColumnFilter {

	private static final Object NULL_MARKER = new Object();

	@NonNull
	final String column;

	@NonNull
	final Object filtered;

	public ColumnFilter(String axis, Object filtered) {
		// if (Set.of(ICountMeasuresConstants.STAR).contains(axis)) {
		// throw new IllegalArgumentException("Invalid axis for filter: " + axis);
		// }

		this.column = axis;
		this.filtered = filtered;

		if (filtered == null) {
			throw new IllegalArgumentException("'filtered' can not be null");
		} else if (filtered instanceof IAdhocFilter) {
			throw new IllegalArgumentException("'filtered' can not be a: " + IAdhocFilter.class.getSimpleName());
		}
	}

	@Override
	public boolean isNot() {
		return false;
	}

	@Override
	public boolean isAxisEquals() {
		return true;
	}

	@Override
	public Object getFiltered() {
		if (filtered == NULL_MARKER) {
			return null;
		} else {
			return filtered;
		}
	}

	public static class ColumnFilterBuilder {
		public ColumnFilterBuilder matchNull() {
			this.filtered = NULL_MARKER;
			return this;
		}
	}

	public static ColumnFilter isEqualTo(String column, Object filtered ) {
		return ColumnFilter.builder().column(column).filtered(filtered).build();
	}
}
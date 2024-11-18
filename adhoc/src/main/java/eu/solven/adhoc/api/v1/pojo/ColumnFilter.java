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
	final Object matching;

	public ColumnFilter(String axis, Object matching) {
		// if (Set.of(ICountMeasuresConstants.STAR).contains(axis)) {
		// throw new IllegalArgumentException("Invalid axis for filter: " + axis);
		// }

		this.column = axis;
		this.matching = matching;

		if (matching == null) {
			throw new IllegalArgumentException("'filtered' can not be null");
		} else if (matching instanceof IAdhocFilter) {
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
		if (matching == NULL_MARKER) {
			return null;
		} else {
			return matching;
		}
	}

	public static class ColumnFilterBuilder {
		public ColumnFilterBuilder matchNull() {
			this.matching = NULL_MARKER;
			return this;
		}

		public ColumnFilterBuilder matching(Object matching) {
			if (matching == null) {
				return matchNull();
			} else {
				this.matching = matching;
				return this;
			}
		}
	}

	public static ColumnFilter isEqualTo(String column, Object matching ) {
		return ColumnFilter.builder().column(column).matching(matching).build();
	}
}
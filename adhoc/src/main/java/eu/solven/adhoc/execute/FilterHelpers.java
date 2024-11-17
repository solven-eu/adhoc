package eu.solven.adhoc.execute;

import java.util.Map;
import java.util.Objects;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.filters.IAndFilter;
import eu.solven.adhoc.api.v1.filters.IColumnFilter;
import eu.solven.adhoc.api.v1.filters.INotFilter;
import eu.solven.adhoc.api.v1.filters.IOrFilter;
import eu.solven.pepper.logging.PepperLogHelper;

public class FilterHelpers {

	public static boolean match(IAdhocFilter filter, Map<String, ?> input) {
		if (filter.isAnd()) {
			IAndFilter andFilter = (IAndFilter) filter;
			return andFilter.getAnd().stream().allMatch(f -> match(f, input));
		} else if (filter.isOr()) {
			IOrFilter orFilter = (IOrFilter) filter;
			return orFilter.getOr().stream().anyMatch(f -> match(f, input));
		} else if (filter.isAxisEquals()) {
			IColumnFilter equalsFilter = (IColumnFilter) filter;
			Object value = input.get(equalsFilter.getColumn());
			return Objects.equals(value, equalsFilter.getFiltered());
		} else if (filter.isNot()) {
			INotFilter notFilter = (INotFilter) filter;
			return !match(notFilter.getNegated(), input);
		} else {
			throw new UnsupportedOperationException(PepperLogHelper.getObjectAndClass(filter).toString());
		}
	}
}

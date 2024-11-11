package eu.solven.adhoc.api.v1.pojo;

import java.util.Map;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.filters.IAndFilter;
import eu.solven.adhoc.api.v1.filters.IEqualsFilter;
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
			IEqualsFilter orFilter = (IEqualsFilter) filter;
			return orFilter.getFiltered().equals(input.get(orFilter.getAxis()));
		} else {
			throw new UnsupportedOperationException(PepperLogHelper.getObjectAndClass(filter).toString());
		}
	}
}

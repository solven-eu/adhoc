package eu.solven.adhoc.query.filter;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;
import lombok.Builder;

/**
 * Like {@link FilterHelpers} but enabling a custom {@link IFilterOptimizer}.
 */
@Builder
public class FilterUtility {
	final IFilterOptimizer optimizer;

	public ISliceFilter commonAnd(Set<? extends ISliceFilter> filters) {
		if (filters.isEmpty()) {
			return ISliceFilter.MATCH_ALL;
		} else if (filters.size() == 1) {
			return filters.iterator().next();
		}

		Iterator<? extends ISliceFilter> iterator = filters.iterator();
		// Common parts are initialized with all parts of the first filter
		Set<ISliceFilter> commonParts = new LinkedHashSet<>(FilterHelpers.splitAnd(iterator.next()));

		while (iterator.hasNext()) {
			Set<ISliceFilter> nextFilterParts = new LinkedHashSet<>(FilterHelpers.splitAnd(iterator.next()));

			commonParts = Sets.intersection(commonParts, nextFilterParts);
		}

		return FilterBuilder.and(commonParts).optimize(optimizer);
	}

	public ISliceFilter commonOr(ImmutableSet<? extends ISliceFilter> filters) {
		// common `OR` in `a|b` and `a|c` is related with negation of the common `AND` between `!a&!b` and `!a&!c`
		return commonAnd(filters.stream().map(ISliceFilter::negate).collect(Collectors.toSet())).negate();
	}

}

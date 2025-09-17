package eu.solven.adhoc.query.filter;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.util.NotYetImplementedException;
import lombok.experimental.UtilityClass;

/**
 * Helps computing functional equality between {@link ISliceFilter}.
 */
@UtilityClass
@Deprecated(since = "WIP")
public class FilterEquivalencyHelpers {
	/**
	 * 
	 * @param left
	 * @param right
	 * @return true if left and right are functionally equivalent. false does not means they are strictly different.
	 */
	// https://en.wikipedia.org/wiki/Disjunctive_normal_form
	public boolean areEquivalent(ISliceFilter left, ISliceFilter right) {
		ISliceFilter commonAnd = FilterHelpers.commonFilter(Set.of(left, right));

		ISliceFilter leftWithoutCommon = FilterHelpers.stripWhereFromFilter(commonAnd, left);
		ISliceFilter rightWithoutCommon = FilterHelpers.stripWhereFromFilter(commonAnd, right);

		Set<ISliceFilter> dnfLeft = dnf(leftWithoutCommon);
		Set<ISliceFilter> dnfRight = dnf(rightWithoutCommon);

		return dnfLeft.equals(dnfRight);
	}

	/**
	 * 
	 * @param filter
	 * @return a {@link Set} (representing an OR) of ISliceFilter, each being a simple {@link IAndFilter}. By simple, we
	 *         mean each {@link IAndFilter} is over {@link IColumnFilter}, optionally negated.
	 */
	private Set<ISliceFilter> dnf(ISliceFilter filter) {
		if (filter instanceof IColumnFilter column) {
			IValueMatcher valueMatcher = column.getValueMatcher();

			if (valueMatcher instanceof InMatcher in) {
				return in.getOperands()
						.stream()
						.map(o -> ColumnFilter.isEqualTo(column.getColumn(), o))
						.collect(Collectors.toSet());
			} else if (valueMatcher instanceof NotMatcher not && not.getNegated() instanceof InMatcher in) {
				return in.getOperands()
						.stream()
						.map(o -> NotFilter.not(ColumnFilter.isEqualTo(column.getColumn(), o)))
						.collect(Collectors.toSet());
			} else {
				return Set.of(column);
			}
		} else if (filter instanceof IAndFilter and) {
			Set<ISliceFilter> operands = and.getOperands();

			List<Set<ISliceFilter>> operandDnfs = operands.stream().map(s -> dnf(s)).toList();

			Set<List<ISliceFilter>> ors = Sets.cartesianProduct(operandDnfs);

			return ors.stream().map(l -> FilterBuilder.and(l).optimize()).collect(Collectors.toSet());
		} else if (filter instanceof IOrFilter or) {
			Set<ISliceFilter> operands = or.getOperands();

			List<Set<ISliceFilter>> operandDnfs = operands.stream().map(s -> dnf(s)).toList();

			Set<ISliceFilter> ors = operandDnfs.stream().flatMap(s -> s.stream()).collect(Collectors.toSet());

			return ors;
		} else if (filter instanceof INotFilter not) {
			Set<ISliceFilter> negatedDnf = dnf(not.getNegated());

			return negatedDnf.stream().map(NotFilter::not).collect(Collectors.toSet());
		} else {
			throw new NotYetImplementedException("Not managed: %s".formatted(filter));
		}
	}
}

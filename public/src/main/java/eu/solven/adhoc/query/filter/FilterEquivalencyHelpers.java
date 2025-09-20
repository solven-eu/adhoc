/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
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
 * 
 * @author Benoit Lacelle
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

			// Each AND operand is turned into an OR of DNFs
			List<Set<ISliceFilter>> operandDnfs = operands.stream().map(FilterEquivalencyHelpers::dnf).toList();

			// Do the cartesian product between the AND of OR
			// TODO Manage smoothly if the cartesianProduct is too large
			Set<List<ISliceFilter>> ors = Sets.cartesianProduct(operandDnfs);

			// Each cartesianProduct entry is a simple AND
			return ors.stream().map(l -> FilterBuilder.and(l).optimize()).collect(Collectors.toSet());
		} else if (filter instanceof IOrFilter or) {
			Set<ISliceFilter> operands = or.getOperands();

			// Turn each OR operand into an OR of simpler DNFs
			List<Set<ISliceFilter>> operandDnfs = operands.stream().map(FilterEquivalencyHelpers::dnf).toList();

			return operandDnfs.stream().flatMap(Set::stream).collect(Collectors.toSet());
		} else if (filter instanceof INotFilter not) {
			Set<ISliceFilter> negatedDnf = dnf(not.getNegated());

			return negatedDnf.stream().map(NotFilter::not).collect(Collectors.toSet());
		} else {
			throw new NotYetImplementedException("Not managed: %s".formatted(filter));
		}
	}
}

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
package eu.solven.adhoc.atoti.translation;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import com.quartetfs.fwk.filtering.ICondition;
import com.quartetfs.fwk.filtering.ILogicalCondition;
import com.quartetfs.fwk.filtering.IMatchingCondition;
import com.quartetfs.fwk.filtering.impl.AndCondition;
import com.quartetfs.fwk.filtering.impl.ComparisonMatchingCondition;
import com.quartetfs.fwk.filtering.impl.EqualCondition;
import com.quartetfs.fwk.filtering.impl.FalseCondition;
import com.quartetfs.fwk.filtering.impl.InCondition;
import com.quartetfs.fwk.filtering.impl.OrCondition;
import com.quartetfs.fwk.filtering.impl.TrueCondition;

import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.value.ComparingMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import lombok.extern.slf4j.Slf4j;

/**
 * Works with com.quartetfs.fwk.filtering.{@link ICondition} but not
 * com.qfs.condition.{@link com.qfs.condition.ICondition}.
 *
 * Typically used for cube filtering.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class AtotiConditionCubeToAdhoc {

	public IAdhocFilter convertToAdhoc(String level, Object rawCondition) {
		if (rawCondition == null) {
			return ColumnFilter.builder().column(level).matchNull().build();
		} else if (rawCondition instanceof com.qfs.condition.ICondition apCondition) {
			log.warn("We should not receive Datastore/RecordQuery conditions: {}", apCondition);
			return ColumnFilter.builder().column(level).matchEquals(apCondition.toString()).build();
		} else if (rawCondition instanceof ICondition apCondition) {
			if (rawCondition instanceof IMatchingCondition matchingCondition) {
				if (matchingCondition instanceof EqualCondition apEqualCondition) {
					return ColumnFilter.isEqualTo(level, apEqualCondition.getMatchingParameter());
				} else if (matchingCondition instanceof InCondition apInCondition) {
					Set<?> domain = (Set<?>) apInCondition.getMatchingParameter();
					return ColumnFilter.builder().column(level).matchIn(domain).build();
				} else if (matchingCondition instanceof ComparisonMatchingCondition apComparisonCondition) {
					return convertComparisonMatchingCondition(level, apComparisonCondition);
				} else {
					// ToStringEquals, Like, Comparison
					log.warn("This case is not well handled: {}", matchingCondition);
					return ColumnFilter.builder().column(level).matchEquals(matchingCondition.toString()).build();
				}
			} else if (rawCondition instanceof ILogicalCondition logicalCondition) {
				return toAdhoc(level, logicalCondition);
			} else {
				// SubCondition
				log.warn("This case is not well handled: {}", apCondition);
				return ColumnFilter.builder().column(level).matchEquals(apCondition.toString()).build();
			}
		} else {
			// Assume we received a raw object
			return ColumnFilter.builder().column(level).matching(rawCondition).build();
		}
	}

	public IAdhocFilter toAdhoc(String level, ILogicalCondition logicalCondition) {
		if (logicalCondition instanceof FalseCondition) {
			return ColumnFilter.MATCH_NONE;
		} else if (logicalCondition instanceof TrueCondition) {
			return ColumnFilter.MATCH_ALL;
		} else if (logicalCondition instanceof OrCondition orCondition) {
			List<IAdhocFilter> subAdhocConditions = Stream.of(orCondition.getConditions())
					.map(subCondition -> convertToAdhoc(level, subCondition))
					.toList();
			return OrFilter.or(subAdhocConditions);
		} else if (logicalCondition instanceof AndCondition andCondition) {
			List<IAdhocFilter> subAdhocConditions = Stream.of(andCondition.getConditions())
					.map(subCondition -> convertToAdhoc(level, subCondition))
					.toList();
			return AndFilter.and(subAdhocConditions);
		} else {
			// Or, And, Not
			log.warn("This case is not well handled: {}", logicalCondition);
			return ColumnFilter.builder().column(level).matchEquals(logicalCondition.toString()).build();
		}
	}

	public ColumnFilter convertComparisonMatchingCondition(String level,
			ComparisonMatchingCondition apComparisonCondition) {
		Object operand = apComparisonCondition.getMatchingParameter();
		boolean greaterThan;
		boolean matchIfEqual;

		// TODO It is unclear how we should matchNull from ActivePivot
		boolean matchIfNull = getMatchIfNull(level, apComparisonCondition);

		switch (apComparisonCondition.getType()) {
		case IMatchingCondition.GREATER:
			greaterThan = true;
			matchIfEqual = false;
			break;
		case IMatchingCondition.LOWER:
			greaterThan = false;
			matchIfEqual = false;
			break;
		case IMatchingCondition.GREATEREQUAL:
			greaterThan = true;
			matchIfEqual = true;
			break;
		case IMatchingCondition.LOWEREQUAL:
			greaterThan = false;
			matchIfEqual = true;
			break;
		default:
			throw new IllegalStateException("Unexpected value: " + apComparisonCondition.getType());
		}
		IValueMatcher valueMatcher = ComparingMatcher.builder()
				.matchIfEqual(matchIfEqual)
				.matchIfNull(matchIfNull)
				.greaterThan(greaterThan)
				.operand(operand)
				.build();
		return ColumnFilter.builder().column(level).valueMatcher(valueMatcher).build();
	}

	protected boolean getMatchIfNull(String level, ComparisonMatchingCondition apComparisonCondition) {
		// BEWARE False on all cases in a first implementation
		return false;
	}
}

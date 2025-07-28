/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.query.filter.value;

import java.util.Comparator;

import eu.solven.adhoc.map.ComparableElseClassComparatorV2;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.query.filter.ColumnFilter;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * To be used with {@link ColumnFilter}, for comparison-based matchers. This works only on naturally comparable objects.
 * 
 * If objects have different types, the comparison always returns false.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
public final class ComparingMatcher implements IValueMatcher, IColumnToString {
	// Can not be an IValueMatcher
	@NonNull
	Object operand;

	// else lowerThan
	boolean greaterThan;

	// else matchIfStrictlyLower|Greater
	boolean matchIfEqual;

	boolean matchIfNull;

	@Builder(toBuilder = true)
	@Jacksonized
	private ComparingMatcher(Object operand, boolean greaterThan, boolean matchIfEqual, boolean matchIfNull) {
		this.operand = AdhocPrimitiveHelpers.normalizeValue(operand);
		this.greaterThan = greaterThan;
		this.matchIfEqual = matchIfEqual;
		this.matchIfNull = matchIfNull;
	}

	/**
	 * Return a {@link Comparable} adapter which accepts null values and sorts them higher than non-null values.
	 * 
	 * @see Comparator#nullsLast(Comparator)
	 */
	// see org.springframework.util.comparator.Comparators
	private static <T> Comparator<T> nullsHigh() {
		Comparator<T> naturalOrderComparator = (Comparator<T>) Comparator.naturalOrder();
		return Comparator.nullsLast(naturalOrderComparator);
	}

	@Override
	public boolean match(Object value) {
		if (value == null) {
			return matchIfNull;
		} else if (matchIfEqual && value.equals(operand)) {
			return true;
		}

		Object normalizedValue = AdhocPrimitiveHelpers.normalizeValue(value);

		if (normalizedValue.getClass() != operand.getClass()) {
			return false;
		} else {
			int compare = ComparableElseClassComparatorV2.doCompare(nullsHigh(), normalizedValue, operand);

			if (matchIfEqual && compare == 0) {
				// equal case may not be captured by previous branch if the value needed normalization
				return true;
			}

			if (greaterThan) {
				return compare > 0;
			} else {
				return compare < 0;
			}
		}

	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		if (greaterThan) {
			sb.append('>');
		} else {
			sb.append('<');
		}
		if (matchIfEqual) {
			sb.append('=');
		}
		sb.append(operand);

		if (matchIfNull) {
			sb.append("|null");
		}

		return sb.toString();
	}

	/**
	 * Lombok @Builder
	 * 
	 * @author Benoit Lacelle
	 */
	public static class ComparingMatcherBuilder {
		@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
		boolean greaterThan;

		public ComparingMatcherBuilder greaterThan(boolean greaterElseLower) {
			this.greaterThan = greaterElseLower;

			return this;
		}

		public ComparingMatcherBuilder greaterThan() {
			return this.greaterThan(true);
		}

		public ComparingMatcherBuilder lowerThan() {
			return this.lowerThan(true);
		}

		public ComparingMatcherBuilder greaterThan(Comparable<?> comparable) {
			return this.greaterThan(true).operand(comparable);
		}

		public ComparingMatcherBuilder lowerThan(Comparable<?> comparable) {
			return this.greaterThan(false).operand(comparable);
		}

	}

	@Override
	public String toString(String column, boolean negated) {
		if (negated) {
			return column + this.toBuilder().greaterThan(!this.isGreaterThan()).build().toString();
		} else {
			return column + this.toString();
		}
	}

	public static IValueMatcher greaterThanOrEqual(Comparable<?> comparable) {
		return ComparingMatcher.builder().greaterThan(comparable).matchIfEqual(true).build();
	}

	public static IValueMatcher strictlyGreaterThan(Comparable<?> comparable) {
		return ComparingMatcher.builder().greaterThan(comparable).matchIfEqual(false).build();
	}

	public static IValueMatcher lowerThanOrEqual(Comparable<?> comparable) {
		return ComparingMatcher.builder().lowerThan(comparable).matchIfEqual(true).build();
	}

	public static IValueMatcher strictlyLowerThan(Comparable<?> comparable) {
		return ComparingMatcher.builder().lowerThan(comparable).matchIfEqual(false).build();
	}
}

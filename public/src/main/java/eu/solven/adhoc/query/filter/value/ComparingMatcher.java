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
import eu.solven.adhoc.query.filter.ColumnFilter;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * To be used with {@link ColumnFilter}, for comparison-based matchers. This works only on naturally comparable objects
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
public class ComparingMatcher implements IValueMatcher {
	// Can not be an IValueMatcher
	@NonNull
	Object operand;

	// else lowerThan
	boolean greaterThan;

	// else lowerThan
	boolean matchIfEqual;

	// else lowerThan
	boolean matchIfNull;

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
		} else {
			int compare = ComparableElseClassComparatorV2.doCompare(nullsHigh(), value, operand);
			if (greaterThan) {
				return compare > 0;
			} else {
				return compare < 0;
			}
		}
	}
}

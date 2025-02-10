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

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import eu.solven.adhoc.query.filter.value.AndMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.pepper.core.PepperLogHelper;

/**
 * Various helpers for {@link IAdhocFilter}
 */
public class AdhocFilterHelpers {
	protected AdhocFilterHelpers() {
		// hidden
	}

	/**
	 *
	 * @param filter
	 *            some {@link IAdhocFilter}, potentially complex
	 * @param column
	 *            some specific column
	 * @return
	 */
	public Optional<IValueMatcher> getValueMatcher(IAdhocFilter filter, String column) {
		if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter) {
			if (columnFilter.getColumn().equals(column)) {
				return Optional.of(columnFilter.getValueMatcher());
			} else {
				// column is not filtered
				return Optional.empty();
			}
		} else if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
			Set<IValueMatcher> filters = andFilter.getOperands()
					.stream()
					.map(f -> getValueMatcher(f, column))
					.flatMap(Optional::stream)
					.collect(Collectors.toSet());

			if (filters.isEmpty()) {
				return Optional.empty();
			} else if (filters.size() == 1) {
				return filters.stream().findFirst();
			} else {
				return Optional.of(AndMatcher.builder().operands(filters).build());
			}
		} else {
			throw new UnsupportedOperationException(
					"filter=%s is not managed yet".formatted(PepperLogHelper.getObjectAndClass(filter)));
		}
	}
	//
	//
	// /**
	// *
	// * @param filter some {@link IAdhocFilter}, potentially complex
	// * @param column some specific column
	// * @param clazz the type of the expected value. Will throw if there is a filter with a not compatible type
	// * @return
	// * @param <T>
	// */
	// public <T> Optional<T> getEqualsOperand(IAdhocFilter filter, String column, Class<? extends T> clazz) {
	// Optional<IValueMatcher> optValueMatcher = getValueMatcher(filter, column);
	//
	// return optvalueMatcher.map(valueMatcher -> {
	// if (valueMatcher instanceof EqualsMatcher equalsMatcher) {
	// Object rawOperand = equalsMatcher.getOperand();
	//
	// if (clazz.isInstance(rawOperand)) {
	// T operandAsT = clazz.cast (rawOperand);
	// return Optional.of(operandAsT);
	// } else {
	// throw new IllegalArgumentException("operand=%s is not instance of
	// %s".formatted(PepperLogHelper.getObjectAndClass(rawOperand), clazz));
	// }
	// } else {
	// // column is filtered but not with equals
	// return Optional.empty();
	// }
	// });
	// }
}

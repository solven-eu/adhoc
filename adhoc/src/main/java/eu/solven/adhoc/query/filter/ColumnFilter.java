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
package eu.solven.adhoc.query.filter;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;

import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.NotValueFilter;
import eu.solven.adhoc.query.filter.value.NullMatcher;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
public class ColumnFilter implements IColumnFilter {

	@NonNull
	final String column;

	@NonNull
	final IValueMatcher valueMatcher;

	// If the input lacks the column, should we behave as if containing an explicit null, or return false.
	// This flag does not have an effect on all IValueMatcher, but only on matchers which may returns true on null.
	// For instance, it is noop for EqualsMatcher, which accepts only a non-null operand.
	@Builder.Default
	final boolean nullIfAbsent = true;

	@Override
	public boolean isNot() {
		return false;
	}

	@Override
	public boolean isColumnFilter() {
		return true;
	}

	@Override
	public IValueMatcher getValueMatcher() {
		return valueMatcher;
	}

	public String toString() {
		if (valueMatcher instanceof EqualsMatcher equalsMatcher) {
			return "%s=%s".formatted(column, equalsMatcher.getOperand());
		} else {
			if (valueMatcher.match(null)) {
				return "%s matches `%s` (nullIfAbsent=%s)".formatted(column, valueMatcher, nullIfAbsent);
			} else {
				// null being not matched, there is no point in logging about `nullIfAbsent`
				return "%s matches `%s`".formatted(column, valueMatcher);
			}
		}
	}

	public static class ColumnFilterBuilder {
		/**
		 * Used when we want this to match cases where the column is null
		 *
		 * @return
		 */
		public ColumnFilterBuilder matchNull() {
			this.valueMatcher = NullMatcher.matchNull();
			return this;
		}

		public ColumnFilterBuilder matchIn(Collection<?> collection) {
			// One important edge-case is getting away from java.util.Set which generates NPE on .contains(null)
			// https://github.com/adoptium/adoptium-support/issues/1186
			Set<Object> operands = collection.stream().map(o -> {
				if (o == null) {
					return NullMatcher.matchNull();
				} else {
					return o;
				}
			}).collect(Collectors.toSet());
			this.valueMatcher = InMatcher.builder().operands(operands).build();
			return this;
		}

		public ColumnFilterBuilder matchEquals(Object o) {
			this.valueMatcher = EqualsMatcher.builder().operand(o).build();
			return this;
		}

		public ColumnFilterBuilder matching(Object matching) {
			if (matching == null) {
				return matchNull();
			} else if (matching instanceof IValueMatcher vm) {
				this.valueMatcher = vm;
				return this;
			} else if (matching instanceof Collection<?> c) {
				return matchIn(c);
			} else if (matching instanceof IColumnFilter) {
				throw new IllegalArgumentException("Can not use a columnFilter as valueFilter: %s"
						.formatted(PepperLogHelper.getObjectAndClass(matching)));
			} else {
				if (matching instanceof Pattern) {
					// May happen due to sick API in LikeMatcher
					throw new IllegalArgumentException(
							"Invalid matching: %s".formatted(PepperLogHelper.getObjectAndClass(matching)));
				}
				return matchEquals(matching);
			}
		}
	}

	/**
	 * @param column
	 *            the name of the filtered column.
	 * @param matching
	 *            the value expected to be found
	 * @return true if the column holds the same value as matching (following {@link Object#equals(Object)}) contract.
	 */
	public static ColumnFilter isEqualTo(String column, Object matching) {
		return ColumnFilter.builder().column(column).matchEquals(matching).build();
	}

	/**
	 * @param column
	 * @param matching
	 * @return true if the value is different, including if the value is null.
	 */
	// https://stackoverflow.com/questions/36508815/not-equal-and-null-in-postgres
	public static ColumnFilter isDistinctFrom(String column, Object matching) {
		NotValueFilter not =
				NotValueFilter.builder().negated(EqualsMatcher.builder().operand(matching).build()).build();
		return ColumnFilter.builder().column(column).matching(not).build();
	}

	/**
	 * @param column
	 * @param first
	 *            a first argument. If it is a {@link List}, it will be unnested.
	 * @param more
	 * @return a {@link ColumnFilter}
	 */
	// https://duckdb.org/docs/sql/query_syntax/unnest.html
	public static IAdhocFilter isIn(String column, Object first, Object... more) {
		List<Object> rawList = Lists.asList(first, more);

		List<Object> expandedList = rawList.stream().flatMap(o -> {
			if (o instanceof Collection<?> c) {
				return c.stream();
			} else {
				return Stream.of(o);
			}
		}).toList();

		if (expandedList.isEmpty()) {
			return IAdhocFilter.MATCH_NONE;
		} else {
			return ColumnFilter.builder().column(column).matchIn(expandedList).build();
		}
	}
}
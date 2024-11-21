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
package eu.solven.adhoc.api.v1.pojo;

import java.util.Collection;
import java.util.stream.Collectors;

import eu.solven.adhoc.api.v1.filters.IColumnFilter;
import eu.solven.adhoc.api.v1.pojo.value.EqualsMatcher;
import eu.solven.adhoc.api.v1.pojo.value.IValueMatcher;
import eu.solven.adhoc.api.v1.pojo.value.InMatcher;
import eu.solven.adhoc.api.v1.pojo.value.NotValueFilter;
import eu.solven.adhoc.api.v1.pojo.value.NullMatcher;
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

	// if the tested input is null, we return this flag
	@Builder.Default
	final boolean nullIfAbsent = true;

	@Override
	public boolean isNot() {
		return false;
	}

	@Override
	public boolean isColumnMatcher() {
		return true;
	}

	@Override
	public IValueMatcher getValueMatcher() {
		return valueMatcher;
	}

	public static class ColumnFilterBuilder {
		public ColumnFilterBuilder matchNull() {
			this.valueMatcher = NullMatcher.matchNull();
			return this;
		}

		public ColumnFilterBuilder matchIn(Collection<?> collection) {
			// One important edge-case is getting away from java.util.Set which generates NPE on .contains(null)
			this.valueMatcher = InMatcher.builder().operands(collection.stream().map(o -> {
				if (o == null) {
					return NullMatcher.matchNull();
				} else {
					return o;
				}
			}).collect(Collectors.toSet())).build();
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
				return matchEquals(matching);
			}
		}
	}

	/**
	 * If matching is null, prefer
	 *
	 * @param column
	 * @param matching
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
}
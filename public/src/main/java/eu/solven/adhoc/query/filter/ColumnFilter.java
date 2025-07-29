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
import java.util.regex.Pattern;

import com.google.common.collect.Lists;

import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IColumnToString;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.filter.value.NullMatcher;
import eu.solven.adhoc.query.filter.value.RegexMatcher;
import eu.solven.adhoc.util.AdhocCollectionHelpers;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Default implementation for {@link IColumnFilter}.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder(toBuilder = true)
@Jacksonized
@SuppressWarnings("PMD.LinguisticNaming")
public class ColumnFilter implements IColumnFilter {

	@NonNull
	final String column;

	@NonNull
	final IValueMatcher valueMatcher;

	// If the input lacks the column, should we behave as if containing an explicit null, or return false.
	// This flag does not have an effect on most IValueMatcher, but only on matchers which may returns true on null.
	// For instance, it is noop for EqualsMatcher, which accepts only a non-null operand. But it can be
	// very important if one use a NullMatcher.
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

	@Override
	public String toString() {
		if (valueMatcher instanceof IColumnToString customToString) {
			return customToString.toString(column, false);
		} else {
			if (valueMatcher.match(null)) {
				return "%s matches `%s` (nullIfAbsent=%s)".formatted(column, valueMatcher, nullIfAbsent);
			} else {
				// null being not matched, there is no point in logging about `nullIfAbsent`
				return "%s matches `%s`".formatted(column, valueMatcher);
			}
		}
	}

	/**
	 * Lombok Builder.
	 * 
	 * @author Benoit Lacelle
	 */
	public static class ColumnFilterBuilder {
		/**
		 * Used when we want this to match cases where the column is null
		 *
		 * @return
		 */
		// Better than `.matching(null)` for not forcing called to use null-references.
		public ColumnFilterBuilder matchNull() {
			this.valueMatcher = NullMatcher.matchNull();
			return this;
		}

		public ColumnFilterBuilder matchIn(Collection<?> operands) {
			this.valueMatcher = InMatcher.isIn(operands);
			return this;
		}

		public ColumnFilterBuilder matchEquals(Object o) {
			this.valueMatcher = EqualsMatcher.isEqualTo(o);
			return this;
		}

		/**
		 * 
		 * @param matching
		 *            may be null, a {@link Collection}, a {@link IValueMatcher}, or any other value for a
		 *            {@link EqualsMatcher}.
		 * @return the {@link ColumnFilterBuilder} instance
		 */
		public ColumnFilterBuilder matching(Object matching) {
			this.valueMatcher = IValueMatcher.matching(matching);
			return this;
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
	public static ISliceFilter isDistinctFrom(String column, Object matching) {
		NotMatcher not = NotMatcher.builder().negated(EqualsMatcher.isEqualTo(matching)).build();
		return isMatching(column, not);
	}

	/**
	 * @param column
	 * @param first
	 *            a first argument. If it is a {@link List}, it will be unnested.
	 * @param more
	 *            If it is a {@link List}, it will be unnested.
	 * @return a {@link ColumnFilter}
	 */
	// https://duckdb.org/docs/sql/query_syntax/unnest.html
	public static ISliceFilter isIn(String column, Object first, Object... more) {
		List<Object> rawList = Lists.asList(first, more);

		Collection<?> expandedList = AdhocCollectionHelpers.unnestAsCollection(rawList);

		if (expandedList.isEmpty()) {
			return MATCH_NONE;
		} else {
			return ColumnFilter.builder().column(column).matchIn(expandedList).build();
		}
	}

	public static ISliceFilter isLike(String column, String likeExpression) {
		return isMatching(column, LikeMatcher.matching(likeExpression));
	}

	public static ISliceFilter isMatching(String column, Pattern pattern) {
		return isMatching(column, RegexMatcher.matching(pattern));
	}

	public static ISliceFilter isMatching(String column, IValueMatcher matcher) {
		return ColumnFilter.builder().column(column).valueMatcher(matcher).build();
	}
}
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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;

import eu.solven.adhoc.query.filter.value.AndMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility method to help doing operations on {@link IAdhocFilter}.
 * 
 * @author Benoit Lacelle
 * @see SimpleFilterEditor for write operations.
 */
@Slf4j
public class FilterHelpers {

	/**
	 *
	 * @param filter
	 *            some {@link IAdhocFilter}, potentially complex
	 * @param column
	 *            some specific column
	 * @return a perfectly matching {@link IValueMatcher} for given column. By perfect, we mean the provided
	 *         {@link IValueMatcher} covers exactly the {@link IAdhocFilter} along given column. This is typically false
	 *         for `OrFilter`.
	 */
	public static IValueMatcher getValueMatcher(IAdhocFilter filter, String column) {
		return getValueMatcherLax(filter, column, true);
	}

	public static IValueMatcher getValueMatcherLax(IAdhocFilter filter, String column) {
		return getValueMatcherLax(filter, column, false);
	}

	private static IValueMatcher getValueMatcherLax(IAdhocFilter filter, String column, boolean throwOnOr) {
		if (filter.isMatchAll()) {
			return IValueMatcher.MATCH_ALL;
		} else if (filter.isMatchNone()) {
			return IValueMatcher.MATCH_NONE;
		} else if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter) {
			if (columnFilter.getColumn().equals(column)) {
				return columnFilter.getValueMatcher();
			} else {
				// column is not filtered
				return IValueMatcher.MATCH_ALL;
			}
		} else if (filter.isNot() && filter instanceof INotFilter notFilter) {
			IAdhocFilter negated = notFilter.getNegated();

			// Some INotFilter may not be optimized into `matchAll` or `matchNone`
			// We analyse these cases manually as we'll later keep `matchAll` if current column is unrelated to the
			// filter
			if (negated.isMatchAll()) {
				return IValueMatcher.MATCH_NONE;
			} else if (negated.isMatchNone()) {
				return IValueMatcher.MATCH_ALL;
			}
			IValueMatcher valueMatcher = getValueMatcherLax(negated, column, throwOnOr);

			if (IValueMatcher.MATCH_ALL.equals(valueMatcher)) {
				// The underlying filter is unrelated to `column`: should not negate `matchAll`
				return IValueMatcher.MATCH_ALL;
			}

			// This is a not trivial valueMatcher: negate it
			return NotMatcher.not(valueMatcher);
		} else if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
			Set<IValueMatcher> filters = andFilter.getOperands()
					.stream()
					.map(f -> getValueMatcherLax(f, column, throwOnOr))
					.collect(Collectors.toSet());

			if (filters.isEmpty()) {
				// This is a weird case as it should have been caught be initial `isMatchAll`
				log.warn("Please report this case: column={} filter={}", column, filter);
				return IValueMatcher.MATCH_ALL;
			} else {
				return AndMatcher.and(filters);
			}
		} else if (filter.isOr() && filter instanceof IOrFilter orFilter) {
			Set<IValueMatcher> matchers = orFilter.getOperands()
					.stream()
					.map(f -> getValueMatcherLax(f, column, throwOnOr))
					.collect(Collectors.toSet());

			if (matchers.isEmpty()) {
				// This is a weird case as it should have been caught be initial `isMatchNone`
				log.warn("Please report this case: column={} filter={}", column, filter);
				return IValueMatcher.MATCH_ALL;
			} else if (matchers.size() == 1 && matchers.contains(IValueMatcher.MATCH_ALL)) {
				// There is an OR, but it does not refer the requested column
				return IValueMatcher.MATCH_ALL;
			} else {
				if (throwOnOr) {
					throw new UnsupportedOperationException(
							"filter=%s is not managed".formatted(PepperLogHelper.getObjectAndClass(filter)));
				}

				return OrMatcher.or(matchers.stream().filter(m -> !m.equals(IValueMatcher.MATCH_ALL)).toList());
			}
		} else {
			throw new UnsupportedOperationException(
					"filter=%s is not managed yet".formatted(PepperLogHelper.getObjectAndClass(filter)));
		}
	}

	public static Map<String, Object> asMap(IAdhocFilter slice) {
		if (slice.isMatchAll()) {
			return Map.of();
		} else if (slice.isColumnFilter() && slice instanceof IColumnFilter columnFilter) {
			IValueMatcher valueMatcher = columnFilter.getValueMatcher();
			if (valueMatcher instanceof EqualsMatcher equalsMatcher) {
				return Map.of(columnFilter.getColumn(), equalsMatcher.getWrapped());
			} else {
				throw new UnsupportedOperationException("Not managed yet: %s".formatted(slice));
			}
		} else if (slice.isAnd() && slice instanceof IAndFilter andFilter) {
			if (andFilter.getOperands().stream().anyMatch(f -> !f.isColumnFilter())) {
				throw new IllegalArgumentException("Only AND of IColumnMatcher can be turned into a Map");
			}
			List<IColumnFilter> columnMatchers = andFilter.getOperands().stream().map(f -> (IColumnFilter) f).toList();
			if (columnMatchers.stream().anyMatch(f -> !(f.getValueMatcher() instanceof EqualsMatcher))) {
				throw new IllegalArgumentException(
						"Only AND of EqualsMatcher can be turned into a Map. Got filter=%s".formatted(columnMatchers));
			}
			Map<String, Object> asMap = new LinkedHashMap<>();

			columnMatchers.forEach(columnFilter -> asMap.put(columnFilter.getColumn(),
					((EqualsMatcher) columnFilter.getValueMatcher()).getWrapped()));

			return asMap;
		} else if (slice.isOr()) {
			throw new IllegalArgumentException("OrMatcher can not be turned into a Map");
		} else {
			throw new UnsupportedOperationException("Not managed yet: %s".formatted(slice));
		}
	}

	public static Set<String> getFilteredColumns(IAdhocFilter filter) {
		if (filter.isMatchAll() || filter.isMatchNone()) {
			return Set.of();
		} else {
			if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter) {
				return Set.of(columnFilter.getColumn());
			} else if (filter.isNot() && filter instanceof INotFilter notFilter) {
				return getFilteredColumns(notFilter.getNegated());
			} else if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
				return andFilter.getOperands()
						.stream()
						.flatMap(operand -> getFilteredColumns(operand).stream())
						.collect(Collectors.toSet());
			} else if (filter.isOr() && filter instanceof IOrFilter orFilter) {
				return orFilter.getOperands()
						.stream()
						.flatMap(operand -> getFilteredColumns(operand).stream())
						.collect(Collectors.toSet());
			} else {
				throw new UnsupportedOperationException(
						"Not managed yet: %s".formatted(PepperLogHelper.getObjectAndClass(filter)));
			}
		}
	}

	public static IValueMatcher wrapWithToString(IValueMatcher valueMatcher, Supplier<String> toString) {
		return new IValueMatcher() {
			@Override
			public boolean match(Object value) {
				return valueMatcher.match(value);
			}

			@JsonProperty(access = JsonProperty.Access.READ_ONLY, value = "wrapped")
			@Override
			public String toString() {
				return toString.get();
			}
		};
	}
}

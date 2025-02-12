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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import eu.solven.adhoc.query.filter.value.AndMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.table.transcoder.IAdhocTableTranscoder;
import eu.solven.adhoc.table.transcoder.IdentityImplicitTranscoder;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility method to help doing operations on {@link IAdhocFilter}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class FilterHelpers {

	/**
	 * 
	 * @param filter
	 * @param input
	 * @return true if the input matches the filter
	 */
	public static boolean match(IAdhocFilter filter, Map<String, ?> input) {
		return match(new IdentityImplicitTranscoder(), filter, input);
	}

	/**
	 * 
	 * @param transcoder
	 * @param filter
	 * @param input
	 * @return true if the input matches the filter, where each column in input is transcoded.
	 */
	public static boolean match(IAdhocTableTranscoder transcoder, IAdhocFilter filter, Map<String, ?> input) {
		if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
			return andFilter.getOperands().stream().allMatch(f -> match(transcoder, f, input));
		} else if (filter.isOr() && filter instanceof IOrFilter orFilter) {
			return orFilter.getOperands().stream().anyMatch(f -> match(transcoder, f, input));
		} else if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter) {
			String underlyingColumn = transcoder.underlyingNonNull(columnFilter.getColumn());
			Object value = input.get(underlyingColumn);

			if (value == null) {
				if (input.containsKey(underlyingColumn)) {
					log.trace("Key to null-ref");
				} else {
					log.trace("Missing key");
					if (columnFilter.isNullIfAbsent()) {
						log.trace("Treat absent as null");
					} else {
						log.trace("Do not treat absent as null, but as missing hence not acceptable");
						return false;
					}
				}
			}

			return columnFilter.getValueMatcher().match(value);
		} else if (filter.isNot() && filter instanceof INotFilter notFilter) {
			return !match(transcoder, notFilter.getNegated(), input);
		} else {
			throw new UnsupportedOperationException(PepperLogHelper.getObjectAndClass(filter).toString());
		}
	}

	/**
	 *
	 * @param filter
	 *            some {@link IAdhocFilter}, potentially complex
	 * @param column
	 *            some specific column
	 * @return
	 */
	public static IValueMatcher getValueMatcher(IAdhocFilter filter, String column) {
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
		} else if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
			Set<IValueMatcher> filters =
					andFilter.getOperands().stream().map(f -> getValueMatcher(f, column)).collect(Collectors.toSet());

			if (filters.isEmpty()) {
				// This is a weird case as it should have been caught be initial `isMatchAll`
				log.warn("Please report this case: column={} filter={}", column, filter);
				return IValueMatcher.MATCH_ALL;
			} else if (filters.size() == 1) {
				return filters.stream().findFirst().get();
			} else {
				return AndMatcher.and(filters);
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

	public static Map<String, Object> asMap(IAdhocFilter slice) {
		if (slice.isMatchAll()) {
			return Map.of();
		} else if (slice.isColumnFilter() && slice instanceof IColumnFilter columnFilter) {
			IValueMatcher valueMatcher = columnFilter.getValueMatcher();
			if (valueMatcher instanceof EqualsMatcher equalsMatcher) {
				return Map.of(columnFilter.getColumn(), equalsMatcher.getOperand());
			} else {
				throw new UnsupportedOperationException("Not managed yet: %s".formatted(slice));
			}
		} else if (slice.isAnd() && slice instanceof IAndFilter andFilter) {
			if (andFilter.getOperands().stream().anyMatch(f -> !f.isColumnFilter())) {
				throw new IllegalArgumentException("Only AND of IColumnMatcher can not turned into a Map");
			}
			List<IColumnFilter> columnMatchers = andFilter.getOperands().stream().map(f -> (IColumnFilter) f).toList();
			if (columnMatchers.stream().anyMatch(f -> !(f.getValueMatcher() instanceof EqualsMatcher))) {
				throw new IllegalArgumentException("Only AND of EqualsMatcher can not turned into a Map");
			}
			Map<String, Object> asMap = new HashMap<>();

			columnMatchers.forEach(columnFilter -> asMap.put(columnFilter.getColumn(),
					((EqualsMatcher) columnFilter.getValueMatcher()).getOperand()));

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
				return getFilteredColumns(notFilter);
			} else if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
				return andFilter.getOperands().stream().flatMap(operand -> getFilteredColumns(operand).stream()).collect(Collectors.toSet());
			} else if (filter.isAnd() && filter instanceof IOrFilter orFilter) {
				return orFilter.getOperands().stream().flatMap(operand -> getFilteredColumns(operand).stream()).collect(Collectors.toSet());
			} else {
				throw new UnsupportedOperationException("Not managed yet: %s".formatted(filter));
			}
		}
	}
}

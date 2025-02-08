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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Default implementation for {@link IAndFilter}
 *
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
public class AndFilter implements IAndFilter {

	@Singular
	final List<IAdhocFilter> filters;

	@Override
	public boolean isNot() {
		return false;
	}

	@Override
	public boolean isMatchAll() {
		// An empty AND is considered to match everything
		return filters.isEmpty();
	}

	@Override
	public boolean isAnd() {
		return true;
	}

	@Override
	public List<IAdhocFilter> getOperands() {
		return Collections.unmodifiableList(filters);
	}

	@Override
	public String toString() {
		if (isMatchAll()) {
			return "matchAll";
		}

		int size = filters.size();
		if (size <= 5) {
			return filters.stream().map(Object::toString).collect(Collectors.joining("&"));
		} else {
			ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", size);

			AtomicInteger index = new AtomicInteger();
			filters.stream().limit(5).forEach(filter -> {
				toStringHelper.add("#" + index.getAndIncrement(), filter);
			});

			return toStringHelper.toString();
		}
	}

	public static IAdhocFilter and(IAdhocFilter filter, IAdhocFilter... moreFilters) {
		return and(Lists.asList(filter, moreFilters));
	}

	public static IAdhocFilter and(List<? extends IAdhocFilter> filters) {
		if (filters.stream().anyMatch(IAdhocFilter::isMatchNone)) {
			return MATCH_NONE;
		}

		return andNotOptimized(packColumnFilters(filters));
	}

	// Like `and` but skipping the optimization. May be useful for debugging
	private static IAdhocFilter andNotOptimized(List<? extends IAdhocFilter> filters) {
		if (filters.stream().anyMatch(IAdhocFilter::isMatchNone)) {
			return MATCH_NONE;
		}

		// Skipping matchAll is useful on `.edit`
		List<? extends IAdhocFilter> notMatchAll = filters.stream().filter(f -> !f.isMatchAll()).flatMap(operand -> {
			if (operand instanceof IAndFilter operandIsAnd) {
				// AND of ANDs
				return operandIsAnd.getOperands().stream();
			} else {
				return Stream.of(operand);
			}
		}).collect(Collectors.toList());

		if (notMatchAll.isEmpty()) {
			return IAdhocFilter.MATCH_ALL;
		} else if (notMatchAll.size() == 1) {
			return notMatchAll.getFirst();
		} else {
			return AndFilter.builder().filters(notMatchAll).build();
		}
	}

	private static List<? extends IAdhocFilter> packColumnFilters(List<? extends IAdhocFilter> filters) {
		Map<Boolean, List<IAdhocFilter>> isColumnToFilters =
				filters.stream().collect(Collectors.groupingBy(f -> f instanceof IColumnFilter));

		if (isColumnToFilters.containsKey(true)) {
			// isMatchNone is cross column as a single column not matching anything reject the whole filter
			AtomicBoolean isMatchNone = new AtomicBoolean();

			List<IAdhocFilter> notManaged = new ArrayList<>();
			if (isColumnToFilters.containsKey(false)) {
				notManaged.addAll(isColumnToFilters.get(false));
			}

			Map<String, List<IColumnFilter>> columnToFilters = isColumnToFilters.get(true)
					.stream()
					.map(f -> (IColumnFilter) f)
					// https://stackoverflow.com/questions/44675454/how-to-get-ordered-type-of-map-from-method-collectors-groupingby
					// LinkedHashMap to maintain as much as possible the initial order
					.collect(Collectors.groupingBy(IColumnFilter::getColumn, LinkedHashMap::new, Collectors.toList()));

			ImmutableList.Builder<IAdhocFilter> packedFiltersBuilder = ImmutableList.<IAdhocFilter>builder();
			columnToFilters.forEach((column, columnFilters) -> {
				if (isMatchNone.get()) {
					// Fail-fast
					return;
				}

				// If there is not a single valueMatcher with explicit values, we should add all as not managed
				AtomicBoolean hadSomeAllowedValues = new AtomicBoolean();
				// allowedValues is meaningful only if hadSomeAllowedValues is true
				Set<Object> allowedValues = new HashSet<>();

				List<IColumnFilter> columnNotManaged = new ArrayList<>();

				columnFilters.stream().forEach(columnFilter -> {
					if (isMatchNone.get()) {
						// Fail-fast
						return;
					}

					if (columnFilter.getValueMatcher() instanceof EqualsMatcher equalsMatcher) {
						hadSomeAllowedValues.set(true);
						Object operand = equalsMatcher.getOperand();

						if (allowedValues.isEmpty()) {
							// This is the first accepted values
							allowedValues.add(operand);
						} else {
							if (allowedValues.contains(operand)) {
								if (allowedValues.size() == 1) {
									// Keep the allowed value
									// Happens if we have multiple EqualsMatcher on the same operand
								} else {
									// This is equivalent to a `.retain`
									allowedValues.clear();
									allowedValues.add(operand);
								}
							} else {
								// There is 2 incompatible set
								isMatchNone.set(true);
							}
						}
					} else if (columnFilter.getValueMatcher() instanceof InMatcher inMatcher) {
						hadSomeAllowedValues.set(true);
						Set<?> operands = inMatcher.getOperands();

						if (allowedValues.isEmpty()) {
							// This is the first accepted values
							allowedValues.addAll(operands);
						} else {
							allowedValues.retainAll(operands);
							if (allowedValues.isEmpty()) {
								// There is 2 incompatible set
								isMatchNone.set(true);
							}
						}
					} else {
						columnNotManaged.add(columnFilter);
					}
				});

				if (hadSomeAllowedValues.get()) {
					allowedValues.removeIf(allowedValue -> {
						// If empty, return false, hence do not remove the allowed value
						return columnNotManaged.stream().anyMatch(f -> !f.getValueMatcher().match(allowedValue));
					});

					if (allowedValues.isEmpty()) {
						isMatchNone.set(true);
					} else {

						packedFiltersBuilder.add(ColumnFilter.isIn(column, allowedValues));
					}
				} else {
					notManaged.addAll(columnNotManaged);
				}
			});

			if (isMatchNone.get()) {
				// Typically happens if we have incompatible equals/in constraints
				return Arrays.asList(MATCH_NONE);
			} else {
				return packedFiltersBuilder
						// Add not managed at the end for readability, as they are generally more complex
						.addAll(notManaged)
						.build();
			}
		} else {
			return filters;
		}
	}

	public static IAdhocFilter and(Map<String, IValueMatcher> filters) {
		return and(filters.entrySet()
				.stream()
				.map(e -> ColumnFilter.builder().column(e.getKey()).valueMatcher(e.getValue()).build())
				.collect(Collectors.toList()));
	}

	public static IAdhocFilter andAxisEqualsFilters(Map<String, ?> filters) {
		return and(filters.entrySet()
				.stream()
				.map(e -> ColumnFilter.builder().column(e.getKey()).matching(e.getValue()).build())
				.collect(Collectors.toList()));
	}

}
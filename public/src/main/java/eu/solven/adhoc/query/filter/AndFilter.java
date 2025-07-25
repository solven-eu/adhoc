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
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

/**
 * Default implementation for {@link IAndFilter}.
 *
 * Prefer `.and(...)` to optimize the matcher, except if you need an unoptimized `AndFilter`.
 *
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
@Slf4j
public class AndFilter implements IAndFilter {

	@Singular
	@NonNull
	final ImmutableSet<IAdhocFilter> filters;

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
	public boolean isMatchNone() {
		return filters.stream().anyMatch(IAdhocFilter::isMatchNone);
	}

	@Override
	public boolean isAnd() {
		return true;
	}

	@Override
	public Set<IAdhocFilter> getOperands() {
		return ImmutableSet.copyOf(filters);
	}

	@Override
	public String toString() {
		if (isMatchAll()) {
			return "matchAll";
		}

		int size = filters.size();
		if (size <= AdhocUnsafe.limitOrdinalToString) {
			return filters.stream().map(o -> {
				if (o instanceof OrFilter orFilter) {
					return "(%s)".formatted(orFilter);
				} else {
					return o.toString();
				}
			}).collect(Collectors.joining("&"));
		} else {
			ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", size);

			AtomicInteger index = new AtomicInteger();
			filters.stream().limit(AdhocUnsafe.limitOrdinalToString).forEach(filter -> {
				toStringHelper.add("#" + index.getAndIncrement(), filter);
			});

			return toStringHelper.toString();
		}
	}

	// `first, second, more` syntax to push providing at least 2 arguments
	public static IAdhocFilter and(IAdhocFilter first, IAdhocFilter second, IAdhocFilter... more) {
		if (more.length == 0 && first.equals(second)) {
			return first;
		}
		return and(Lists.asList(first, second, more));
	}

	public static IAdhocFilter and(Collection<? extends IAdhocFilter> filters) {
		if (filters.stream().anyMatch(IAdhocFilter::isMatchNone)) {
			return MATCH_NONE;
		}

		// We need to start by flattening the input (e.g. `AND(AND(a=a1,b=b2)&a=a2)` to `AND(a=a1,b=b2,a=a2)`)
		IAdhocFilter flatten = andNotOptimized(filters);

		// TableQueryOptimizer.canInduce would typically do an `AND` over an `OR`
		// So we need to optimize `(c=c1) AND (c=c1 OR d=d1)`
		if (flatten instanceof IAndFilter andFilter) {
			flatten = optimizeAndOfOr(andFilter);
		}

		if (flatten instanceof IAndFilter andFilter) {
			// Then, we simplify given columnFilters (e.g. `a=a1&a=a2` to `matchNone`)
			Collection<? extends IAdhocFilter> packedColumns = packColumnFilters(andFilter.getOperands());

			// Then simplify the AND (e.g. `AND(a=a1)` to `a=a1`)
			return andNotOptimized(packedColumns);
		} else {
			return flatten;
		}
	}

	// https://en.m.wikipedia.org/wiki/Logic_optimization
	// https://en.m.wikipedia.org/wiki/Quine%E2%80%93McCluskey_algorithm
	// https://en.m.wikipedia.org/wiki/Espresso_heuristic_logic_minimizer
	// BEWARE This algorithm is not smart at all, as it catches only a very limited number of cases.
	// One may contribute a finer implementation.
	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	private static IAdhocFilter optimizeAndOfOr(IAndFilter andFilter) {
		Set<IAdhocFilter> operands = andFilter.getOperands();

		// TODO Manage `Not(Or(...))` and `Column(In(...))`
		Map<Boolean, List<IAdhocFilter>> orNotOr =
				operands.stream().collect(Collectors.partitioningBy(o -> o instanceof OrFilter));

		if (!orNotOr.get(true).isEmpty() && !orNotOr.get(false).isEmpty()) {
			List<List<IAdhocFilter>> orFilters = orNotOr.get(true)
					.stream()
					.map(f -> (OrFilter) f)
					.map(f -> f.getOperands().stream().toList())
					.toList();

			IAdhocFilter and = and(orNotOr.get(false));
			Set<IAdhocFilter> ors = Lists.cartesianProduct(orFilters).stream().map(entry -> {
				IAdhocFilter and2 = and(entry);
				return and(and2, and);
			})
					// We hope a bunch of entry are filtered out
					.filter(f -> !f.isMatchNone())
					.collect(Collectors.toCollection(LinkedHashSet::new));

			Map<IAdhocFilter, IAdhocFilter> inducedToInducer = new LinkedHashMap<>();

			ors.stream().forEach(inducer -> {
				ors.stream()
						// filter is induced is stricter than inducer
						.filter(induced -> induced != inducer && and(induced, inducer).equals(induced))
						.forEach(induced -> {
							// BEWARE an induced may have multiple inducer
							inducedToInducer.put(induced, inducer);
						});
			});

			inducedToInducer.forEach((induced, inducer) -> {
				// Remove entry one by one, else we fear we may remove an inducer
				// BEWARE Is it legit? I mean, should we just remove all inducers in all cases?
				if (ors.contains(inducer)) {
					ors.remove(induced);
				}
			});

			// BEWARE This heuristic is very weak. We should have a clearer score to define which expressions is
			// better/simpler/faster. Generally speaking, we prefer AND over OR.
			IAdhocFilter orCandidate = OrFilter.or(ors);
			if (costFunction(orCandidate) < costFunction(andFilter)) {
				return orCandidate;
			}
		}
		return andFilter;
	}

	static int costFunction(Collection<? extends IAdhocFilter> operands) {
		return operands.stream().mapToInt(AndFilter::costFunction).sum();
	}

	// factors are additive (hence not multiplicative) as we prefer a high `Not` (counting once) than multiple deep Not
	@SuppressWarnings("checkstyle:MagicNumber")
	static int costFunction(IAdhocFilter f) {
		if (f instanceof IAndFilter andFilter) {
			return costFunction(andFilter.getOperands());
		} else if (f instanceof INotFilter notFilter) {
			// `Not` costs 3: we prefer one OR than one NOT
			return 3 + costFunction(notFilter.getNegated());
		} else if (f instanceof IColumnFilter columnFilter && columnFilter.getValueMatcher() instanceof NotMatcher) {
			// `Not` costs 3: we prefer one OR than one NOT
			return 3;
		} else if (f instanceof IOrFilter orFilter) {
			return 2 + costFunction(orFilter.getOperands());
		} else {
			// ColumnFilter
			return 1;
		}
	}

	// Like `and` but skipping the optimization. May be useful for debugging
	private static IAdhocFilter andNotOptimized(Collection<? extends IAdhocFilter> filters) {
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
			return MATCH_ALL;
		} else if (notMatchAll.size() == 1) {
			return notMatchAll.getFirst();
		}

		IAdhocFilter orCandidate =
				OrFilter.builder().filters(notMatchAll.stream().map(NotFilter::not).toList()).build();
		IAdhocFilter notOrCandidate = NotFilter.builder().negated(orCandidate).build();
		if (costFunction(notOrCandidate) < costFunction(notMatchAll)) {
			return notOrCandidate;
		}

		return builder().filters(notMatchAll).build();
	}

	/**
	 * Optimize input filters if they cover same columns. Typically, `a=a1&a=a2` can be simplified into `matchNone`.
	 * 
	 * @param filters
	 * @return
	 */
	@SuppressWarnings("PMD.CognitiveComplexity")
	private static Collection<? extends IAdhocFilter> packColumnFilters(Collection<? extends IAdhocFilter> filters) {
		@SuppressWarnings("PMD.LinguisticNaming")
		Map<Boolean, List<IAdhocFilter>> isColumnToFilters =
				filters.stream().collect(Collectors.partitioningBy(f -> f instanceof IColumnFilter));

		if (isColumnToFilters.get(true).isEmpty()) {
			// Not a single columnFilter
			return filters;
		} else {
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

				// Do no collect IValueMatcher until managing `nullIfAbsent`
				List<IColumnFilter> columnNotManaged = new ArrayList<>();

				columnFilters.stream().forEach(columnFilter -> {
					if (isMatchNone.get()) {
						// Fail-fast
						return;
					}

					if (columnFilter.getValueMatcher() instanceof EqualsMatcher equalsMatcher) {
						hadSomeAllowedValues.set(true);
						Object operand = equalsMatcher.getWrapped();

						if (allowedValues.isEmpty()) {
							// This is the first accepted values
							allowedValues.add(operand);
						} else {
							if (allowedValues.contains(operand)) {
								if (allowedValues.size() == 1) {
									// Keep the allowed value
									// Happens if we have multiple EqualsMatcher on the same operand
									log.trace("Keep the allowed value");
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
					if (!columnNotManaged.isEmpty()) {
						allowedValues.removeIf(allowedValue -> {
							// If empty, return false, hence do not remove the allowed value
							return columnNotManaged.stream().noneMatch(f -> f.getValueMatcher().match(allowedValue));
						});
					}

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
		}
	}

	/**
	 *
	 * @param columnToFilter
	 *            each key maps to a column, while each value represent a matcher. May be an {@link IValueMatcher}, or a
	 *            {@link Collection}, a `null`, or a value
	 *
	 * @return a filter doing an `AND` between each {@link Map} entry,
	 */
	public static IAdhocFilter and(Map<String, ?> columnToFilter) {
		int size = columnToFilter.size();
		List<ColumnFilter> columnFilters = new ArrayList<>(size);

		columnToFilter.forEach((k, v) -> {
			columnFilters.add(ColumnFilter.builder().column(k).matching(v).build());
		});

		// BEWARE Do not call `.and` due to most optimizations/checks are irrelevant
		// And this is a performance bottleneck in Shiftor
		if (columnFilters.size() == 1) {
			return columnFilters.getFirst();
		} else {
			return builder().filters(columnFilters).build();
		}
	}

}
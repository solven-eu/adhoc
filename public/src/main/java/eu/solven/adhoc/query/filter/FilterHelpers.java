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
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.query.filter.value.AndMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility method to help doing operations on {@link ISliceFilter}.
 * 
 * @author Benoit Lacelle
 * @see SimpleFilterEditor for write operations.
 */
@UtilityClass
@Slf4j
@SuppressWarnings("PMD.GodClass")
public class FilterHelpers {

	/**
	 *
	 * @param filter
	 *            some {@link ISliceFilter}, potentially complex
	 * @param column
	 *            some specific column
	 * @return a perfectly matching {@link IValueMatcher} for given column. By perfect, we mean the provided
	 *         {@link IValueMatcher} covers exactly the {@link ISliceFilter} along given column. This is typically false
	 *         for `OrFilter`.
	 */
	public static IValueMatcher getValueMatcher(ISliceFilter filter, String column) {
		return getValueMatcherLax(filter, column, true);
	}

	/**
	 * 
	 * @param filter
	 * @param column
	 * @return a lax matching {@link IValueMatcher}. By lax, we mean the received filter may be actually applied from
	 *         diverse {@link OrFilter}. In other words, `AND` over the lax {@link IValueMatcher} may not recompose the
	 *         original {@link ISliceFilter} (especially it is is not a simple {@link AndFilter} over
	 *         {@link ColumnFilter}).
	 */
	public static IValueMatcher getValueMatcherLax(ISliceFilter filter, String column) {
		return getValueMatcherLax(filter, column, false);
	}

	/**
	 * 
	 * @param filter
	 * @param column
	 * @param throwOnOr
	 *            if true, we throw if the output {@link IValueMatcher} is not covering the whole matcher (i.e. if it is
	 *            not a `AND`).
	 * @return
	 */
	private static IValueMatcher getValueMatcherLax(ISliceFilter filter, String column, boolean throwOnOr) {
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
		} else {
			Set<ISliceFilter> splitAnds = splitAnd(filter);

			if (splitAnds.isEmpty()) {
				return IValueMatcher.MATCH_ALL;
			} else if (splitAnds.size() == 1) {
				// We receive a plain OR
				Set<ISliceFilter> splitOrs = splitOr(splitAnds.iterator().next());

				if (splitOrs.isEmpty()) {
					return IValueMatcher.MATCH_NONE;
				} else if (splitOrs.size() == 1) {
					throw new UnsupportedOperationException(
							"filter:%s is not managed".formatted(PepperLogHelper.getObjectAndClass(filter)));
				} else {
					Set<IValueMatcher> orMatchers = splitOrs.stream()
							.map(f -> getValueMatcherLax(f, column, throwOnOr))
							// .filter(f -> !IValueMatcher.MATCH_ALL.equals(f))
							.collect(ImmutableSet.toImmutableSet());

					if (orMatchers.size() == 1) {
						// This is a common factor to all OR operands
						return orMatchers.iterator().next();
					} else if (throwOnOr) {
						throw new UnsupportedOperationException(
								"filter:%s is not managed".formatted(PepperLogHelper.getObjectAndClass(filter)));
					} else {
						return OrMatcher.or(orMatchers.stream()
								// .filter(m -> !IValueMatcher.MATCH_ALL.equals(m))
								.toList());
					}
				}
			} else {
				Set<IValueMatcher> matchers = splitAnds.stream()
						.map(f -> getValueMatcherLax(f, column, throwOnOr))
						.filter(f -> !IValueMatcher.MATCH_ALL.equals(f))
						.collect(ImmutableSet.toImmutableSet());

				return AndMatcher.and(matchers);
			}
		}
	}

	public static Map<String, Object> asMap(ISliceFilter slice) {
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

	public static Set<String> getFilteredColumns(ISliceFilter filter) {
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

	public static boolean visit(ISliceFilter filter, IFilterVisitor filterVisitor) {
		if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
			return filterVisitor.testAndOperands(andFilter.getOperands());
		} else if (filter.isOr() && filter instanceof IOrFilter orFilter) {
			return filterVisitor.testOrOperands(orFilter.getOperands());
		} else if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter) {
			return filterVisitor.testColumnOperand(columnFilter);
		} else if (filter.isNot() && filter instanceof INotFilter notFilter) {
			return filterVisitor.testNegatedOperand(notFilter.getNegated());
		} else {
			throw new UnsupportedOperationException("filter=%s".formatted(PepperLogHelper.getObjectAndClass(filter)));
		}
	}

	/**
	 * Given the input filters, considered to be `AND` together, this returns a `WHERE` filter, common to all input
	 * filters. It would typically extracted from each filter with
	 * {@link #stripWhereFromFilter(ISliceFilter, ISliceFilter)}.
	 * 
	 * @param filters
	 * @return
	 */
	public static ISliceFilter commonAnd(Set<? extends ISliceFilter> filters) {
		return new FilterUtility(AdhocUnsafe.sliceFilterOptimizer).commonAnd(filters);
	}

	/**
	 * Given the input filters, considered to be `OR` together, this returns a `WHERE` filter, common to all input
	 * filters.
	 * 
	 * @param filters
	 * @return
	 */
	public static ISliceFilter commonOr(ImmutableSet<? extends ISliceFilter> filters) {
		return new FilterUtility(AdhocUnsafe.sliceFilterOptimizer).commonOr(filters);
	}

	/**
	 * Split the filter in a Set of {@link ISliceFilter}, equivalent by AND to the original filter.
	 * 
	 * @param filter
	 * @return
	 */
	public static Set<ISliceFilter> splitAnd(ISliceFilter filter) {
		return splitAndStream(filter).collect(ImmutableSet.toImmutableSet());
	}

	// OPTIMIZATION: Flatten the whole input into a single Stream before collecting into a Set
	protected static Stream<ISliceFilter> splitAndStream(ISliceFilter filter) {
		if (filter instanceof IAndFilter andFilter) {
			return andFilter.getOperands().stream().flatMap(FilterHelpers::splitAndStream);
		} else if (filter instanceof INotFilter notFilter) {
			if (notFilter.getNegated() instanceof IOrFilter orFilter) {
				return orFilter.getOperands().stream().map(ISliceFilter::negate).flatMap(FilterHelpers::splitAndStream);
			} else if (notFilter.getNegated() instanceof IColumnFilter columnFilter
					&& columnFilter.getValueMatcher() instanceof InMatcher inMatcher) {
				return inMatcher.getOperands()
						.stream()
						.map(o -> ColumnFilter.match(columnFilter.getColumn(), NotMatcher.notEqualTo(o)));
			}
		} else if (filter instanceof IColumnFilter columnFilter) {
			IValueMatcher valueMatcher = columnFilter.getValueMatcher();

			String column = columnFilter.getColumn();
			if (valueMatcher instanceof AndMatcher andMatcher) {
				return andMatcher.getOperands()
						.stream()
						.map(operand -> ColumnFilter.builder().column(column).valueMatcher(operand).build());
			} else if (valueMatcher instanceof NotMatcher notMatcher
					&& notMatcher.getNegated() instanceof InMatcher notInMatcher) {
				return notInMatcher.getOperands()
						.stream()
						.map(operand -> ColumnFilter.builder()
								.column(column)
								.valueMatcher(NotMatcher.not(EqualsMatcher.matchEq(operand)))
								.build());
			}
		}

		// Not splittable
		return Stream.of(filter);
	}

	/**
	 * 
	 * @param filter
	 * @return a Set of {@link ISliceFilter} which, combined with OR, is equivalent to the input
	 */
	// OPTIMIZATION: Flatten the whole input into a single Stream before collecting into a Set
	public static Set<ISliceFilter> splitOr(ISliceFilter filter) {
		return splitOrStream(filter).collect(ImmutableSet.toImmutableSet());
	}

	public static Stream<ISliceFilter> splitOrStream(ISliceFilter filter) {
		if (filter instanceof IOrFilter orFilter) {
			return orFilter.getOperands().stream().flatMap(FilterHelpers::splitOrStream);
		} else if (filter instanceof INotFilter notFilter && notFilter.getNegated() instanceof IAndFilter andFilter) {
			return andFilter.getOperands().stream().map(ISliceFilter::negate).flatMap(FilterHelpers::splitOrStream);
		} else if (filter instanceof IColumnFilter columnFilter) {
			IValueMatcher valueMatcher = columnFilter.getValueMatcher();

			String column = columnFilter.getColumn();
			if (valueMatcher instanceof OrMatcher orMatcher) {
				return orMatcher.getOperands()
						.stream()
						.map(operand -> ColumnFilter.builder().column(column).valueMatcher(operand).build());
			} else if (valueMatcher instanceof InMatcher inMatcher) {
				return inMatcher.getOperands()
						.stream()
						.map(operand -> ColumnFilter.builder().column(column).matchEquals(operand).build());
			}
		}

		// Not splittable
		return Stream.of(filter);
	}

	/**
	 * 
	 * Typically true if `stricter:A=a1&B=b1` and `laxer:B=b1` or `stricter:A=a1` and `laxer:A=a1|B=b1`.
	 * 
	 * True if stricter and laxer are equivalent.
	 * 
	 * @param stricter
	 * @param laxer
	 * @return true if all rows matched by `stricter` are matched by `laxer`.
	 */
	// if `a&b=a` then a is stricter than b, as a is enough to imply b
	// then `a&b|b=a|b`
	// then `b=a|b` then b is laxer than a, as b is enough to cover a
	public static boolean isStricterThan(ISliceFilter stricter, ISliceFilter laxer) {
		return AdhocUnsafe.filterStripperFactory.makeFilterStripper(stricter).isStricterThan(laxer);
	}

	/**
	 * `laxer` is laxer than `stricter` if any row matched by `stricter` it also matched by `laxer`.
	 * 
	 * @param laxer
	 * @param stricter
	 * @return
	 */
	public static boolean isLaxerThan(ISliceFilter laxer, ISliceFilter stricter) {
		return isStricterThan(laxer.negate(), stricter.negate());
	}

	/**
	 * 
	 * @param where
	 *            some `WHERE` clause
	 * @param filter
	 *            some `FILTER` clause
	 * @return an equivalent `FILTER` clause, simplified given the `WHERE` clause, considering the WHERE and FILTER
	 *         clauses are combined with`AND`. `WHERE` may or may not be laxer than `FILTER`. `output&where=filter`
	 */
	public static ISliceFilter stripWhereFromFilter(ISliceFilter where, ISliceFilter filter) {
		return AdhocUnsafe.filterStripperFactory.makeFilterStripper(where).strip(filter);
	}

	/**
	 * Similar to {@link #stripWhereFromFilter(ISliceFilter, ISliceFilter)} but for an OR.
	 * 
	 * @param contribution
	 * @param filter
	 * @return a {@link ISliceFilter} so that `contribution|output=filter`.
	 */
	public static ISliceFilter simplifyOrGivenContribution(ISliceFilter contribution, ISliceFilter filter) {
		// Given `WHERE:a`, turns `FILTER:a|b|c&d` into `FILTER:b|c&d`
		return stripWhereFromFilter(contribution.negate(), filter.negate()).negate();
	}

}

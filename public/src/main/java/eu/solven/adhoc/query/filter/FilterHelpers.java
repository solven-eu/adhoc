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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.query.filter.value.AndMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;
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

	@SuppressWarnings("PMD.CognitiveComplexity")
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
		} else if (filter.isNot() && filter instanceof INotFilter notFilter) {
			ISliceFilter negated = notFilter.getNegated();

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

				return OrMatcher.or(matchers.stream().filter(m -> !IValueMatcher.MATCH_ALL.equals(m)).toList());
			}
		} else {
			throw new UnsupportedOperationException(
					"filter=%s is not managed yet".formatted(PepperLogHelper.getObjectAndClass(filter)));
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
		if (filters.isEmpty()) {
			return ISliceFilter.MATCH_ALL;
		} else if (filters.size() == 1) {
			return filters.iterator().next();
		}

		Iterator<? extends ISliceFilter> iterator = filters.iterator();
		// Common parts are initialized with all parts of the first filter
		Set<ISliceFilter> commonParts = new LinkedHashSet<>(splitAnd(iterator.next()));

		while (iterator.hasNext()) {
			Set<ISliceFilter> nextFilterParts = new LinkedHashSet<>(splitAnd(iterator.next()));

			commonParts = Sets.intersection(commonParts, nextFilterParts);
		}

		return FilterBuilder.and(commonParts).optimize();
	}

	/**
	 * Given the input filters, considered to be `OR` together, this returns a `WHERE` filter, common to all input
	 * filters.
	 * 
	 * @param filters
	 * @return
	 */
	public static ISliceFilter commonOr(ImmutableSet<? extends ISliceFilter> filters) {
		// common `OR` in `a|b` and `a|c` is related with negation of the common `AND` between `!a&!b` and `!a&!c`
		return commonAnd(filters.stream().map(ISliceFilter::negate).collect(Collectors.toSet())).negate();
	}

	/**
	 * Split the filter in a Set of {@link ISliceFilter}, equivalent by AND to the original filter.
	 * 
	 * @param filter
	 * @return
	 */
	public static Set<ISliceFilter> splitAnd(ISliceFilter filter) {
		if (filter.isMatchAll()) {
			return ImmutableSet.of();
		} else if (filter.isMatchNone()) {
			return ImmutableSet.of(filter);
		} else if (filter instanceof IAndFilter andFilter) {
			return andFilter.getOperands()
					.stream()
					.flatMap(f -> splitAnd(f).stream())
					.collect(ImmutableSet.toImmutableSet());
		} else if (filter instanceof INotFilter notFilter) {
			if (notFilter.getNegated() instanceof IOrFilter orFilter) {
				return orFilter.getOperands()
						.stream()
						.map(NotFilter::not)
						.flatMap(f -> splitAnd(f).stream())
						.collect(ImmutableSet.toImmutableSet());
			} else if (notFilter.getNegated() instanceof IColumnFilter columnFilter
					&& columnFilter.getValueMatcher() instanceof InMatcher inMatcher) {
				return inMatcher.getOperands()
						.stream()
						.map(o -> ColumnFilter.match(columnFilter.getColumn(), NotMatcher.notEqualTo(o)))
						.collect(ImmutableSet.toImmutableSet());
			}
		} else if (filter instanceof IColumnFilter columnFilter) {
			IValueMatcher valueMatcher = columnFilter.getValueMatcher();

			String column = columnFilter.getColumn();
			if (valueMatcher instanceof AndMatcher andMatcher) {
				return andMatcher.getOperands()
						.stream()
						.map(operand -> ColumnFilter.builder().column(column).valueMatcher(operand).build())
						.collect(ImmutableSet.toImmutableSet());
			} else if (valueMatcher instanceof NotMatcher notMatcher
					&& notMatcher.getNegated() instanceof InMatcher notInMatcher) {
				return notInMatcher.getOperands()
						.stream()
						.map(operand -> ColumnFilter.builder()
								.column(column)
								.valueMatcher(NotMatcher.not(EqualsMatcher.equalTo(operand)))
								.build())
						.collect(ImmutableSet.toImmutableSet());
			}
		}

		// Not splittable
		return ImmutableSet.of(filter);
	}

	public static Set<ISliceFilter> splitOr(ISliceFilter filter) {
		if (filter.isMatchNone()) {
			return ImmutableSet.of();
		} else if (filter.isMatchAll()) {
			return ImmutableSet.of(filter);
		} else if (filter instanceof IOrFilter orFilter) {
			return orFilter.getOperands()
					.stream()
					.flatMap(f -> splitOr(f).stream())
					.collect(ImmutableSet.toImmutableSet());
		} else if (filter instanceof INotFilter notFilter && notFilter.getNegated() instanceof IAndFilter andFilter) {
			return andFilter.getOperands()
					.stream()
					.map(NotFilter::not)
					.flatMap(f -> splitOr(f).stream())
					.collect(ImmutableSet.toImmutableSet());
		} else if (filter instanceof IColumnFilter columnFilter) {
			IValueMatcher valueMatcher = columnFilter.getValueMatcher();

			String column = columnFilter.getColumn();
			if (valueMatcher instanceof OrMatcher orMatcher) {
				return orMatcher.getOperands()
						.stream()
						.map(operand -> ColumnFilter.builder().column(column).valueMatcher(operand).build())
						.collect(ImmutableSet.toImmutableSet());
			} else if (valueMatcher instanceof InMatcher inMatcher) {
				return inMatcher.getOperands()
						.stream()
						.map(operand -> ColumnFilter.builder().column(column).matchEquals(operand).build())
						.collect(ImmutableSet.toImmutableSet());
			}
		}

		// Not splittable
		return ImmutableSet.of(filter);
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
	public static boolean isStricterThan(ISliceFilter stricter, ISliceFilter laxer) {
		if (stricter instanceof INotFilter notStricter && laxer instanceof INotFilter notLaxer) {
			return isStricterThan(notLaxer.getNegated(), notStricter.getNegated());
		} else if (stricter instanceof IColumnFilter stricterColumn && laxer instanceof IColumnFilter laxerFilter) {
			boolean isSameColumn = stricterColumn.getColumn().equals(laxerFilter.getColumn());
			if (!isSameColumn) {
				return false;
			}
			return isStricterThan(stricterColumn.getValueMatcher(), laxerFilter.getValueMatcher());
		}

		// BEWARE Do not rely on `OrFilter` as this method is called by `AndFilter` optimizations and `OrFilter` also
		// relies on `AndFilter` optimization. Doing so would lead to cycle in the optimizations.
		// BEWARE Do not rely on AndFilter either, else it may lead on further cycles
		// return FilterOptimizerHelpers.and(stricter, laxer).equals(stricter);

		{
			Set<ISliceFilter> allStricters = splitAnd(stricter);
			Set<ISliceFilter> allLaxers = splitAnd(laxer);
			if (allStricters.containsAll(allLaxers)) {
				// true if `stricter:A=a1&B=b1` and `laxer:B=b1`
				return true;
			}
			// true if `a==a1&b==b1&c==c1` and `a=in=(a1,a2)&b=in=(b1,b2)`.
			boolean allLaxersHasStricter = allLaxers.stream().allMatch(oneLaxer -> {
				return allStricters.stream().anyMatch(oneStricter -> {
					if (oneStricter.equals(stricter) && oneLaxer.equals(laxer)) {
						// break cycle in recursivity
						return false;
					}

					return isStricterThan(oneStricter, oneLaxer);
				});
			});
			if (allLaxersHasStricter) {
				return true;
			}
		}

		{
			Set<ISliceFilter> allStricters = splitOr(stricter);
			Set<ISliceFilter> allLaxers = splitOr(laxer);
			if (allLaxers.containsAll(allStricters)) {
				// true if `stricter:A=a1` and `laxer:A=a1|B=b1`.
				return true;
			}

			// true if `stricter:A=7` and `laxer:A>5|B=b1`.
			boolean allLaxersInStricter = allStricters.stream().allMatch(oneStricter -> {
				return allLaxers.stream().anyMatch(oneLaxer -> {
					if (oneStricter.equals(stricter) && oneLaxer.equals(laxer)) {
						// break cycle in recursivity
						return false;
					}

					return isStricterThan(oneStricter, oneLaxer);
				});
			});
			if (allLaxersInStricter) {
				return true;
			}
		}

		return false;
	}

	private static boolean isStricterThan(IValueMatcher stricter, IValueMatcher laxer) {
		return AndMatcher.and(stricter, laxer).equals(stricter);
	}

	/**
	 * 
	 * @param where
	 *            some `WHERE` clause
	 * @param filter
	 *            some `FILTER` clause
	 * @return an equivalent `FILTER` clause, simplified given the `WHERE` clause, considering the WHERE and FILTER
	 *         clauses are combined with`AND`. `WHERE` may or may not be laxer than `FILTER`.
	 */
	public static ISliceFilter stripWhereFromFilter(ISliceFilter where, ISliceFilter filter) {
		if (where.isMatchAll()) {
			// `WHERE` has no clause: `FILTER` has to keep all clauses
			return filter;
		} else if (isStricterThan(where, filter)) {
			// Catch some edge-case like `where.equals(filter)`
			// More generally: if `WHERE && FILTER === WHERE`, then `FILTER` is irrelevant
			return ISliceFilter.MATCH_ALL;
		}

		// Given the FILTER, we reject the AND operands already covered by WHERE
		ISliceFilter postAnd;
		{

			// Split the FILTER in parts
			Set<? extends ISliceFilter> andOperands = splitAnd(filter);

			Set<ISliceFilter> notInWhere = new LinkedHashSet<>();

			// For each part of `FILTER`, reject those already filtered in `WHERE`
			for (ISliceFilter subFilter : andOperands) {
				boolean whereCoversSubFilter = isStricterThan(where, subFilter);

				if (!whereCoversSubFilter) {
					notInWhere.add(subFilter);
				}
			}
			postAnd = FilterBuilder.and(notInWhere).optimize();
		}

		// Given the FILTER, we reject the OR operands already rejected by WHERE
		// TODO Why managing OR after AND? (and not the inverse)
		ISliceFilter postOr;
		{

			Set<ISliceFilter> orOperands = splitOr(postAnd);

			Set<ISliceFilter> notInWhere = new LinkedHashSet<>();
			// For each part of `FILTER`, reject those already filtered in `WHERE`
			for (ISliceFilter subFilter : orOperands) {
				boolean whereRejectsSubFilter =
						ISliceFilter.MATCH_NONE.equals(FilterBuilder.and(where, subFilter).optimize());

				if (!whereRejectsSubFilter) {
					notInWhere.add(subFilter);
				}
			}

			postOr = FilterBuilder.or(notInWhere).optimize();
		}

		return postOr;
	}

	public static ISliceFilter stripWhereFromFilterOr(ISliceFilter where, ISliceFilter filter) {
		// Given `a`, turns `a|b|c&d` into `b|c&d`
		return stripWhereFromFilter(where.negate(), filter.negate()).negate();
	}

}

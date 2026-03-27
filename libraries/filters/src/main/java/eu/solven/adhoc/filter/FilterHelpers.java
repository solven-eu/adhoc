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
package eu.solven.adhoc.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;

import eu.solven.adhoc.filter.stripper.IFilterStripperFactory;
import eu.solven.adhoc.filter.value.AndMatcher;
import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.filter.value.InMatcher;
import eu.solven.adhoc.filter.value.NotMatcher;
import eu.solven.adhoc.filter.value.NullMatcher;
import eu.solven.adhoc.filter.value.OrMatcher;
import eu.solven.adhoc.util.NotYetImplementedException;
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
// `filter=%s` is duplicated
@SuppressWarnings({ "PMD.GodClass", "PMD.AvoidDuplicateLiterals" })
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
		} else if (filter instanceof FlatAndFilter flatAndFilter) {
			return flatAndFilter.columnToMatcher.getOrDefault(column, IValueMatcher.MATCH_ALL);
		} else {
			Set<ISliceFilter> splitAnds = splitAnd(filter);

			if (splitAnds.isEmpty()) {
				return IValueMatcher.MATCH_ALL;
			} else if (splitAnds.size() == 1) {
				// We receive a plain OR
				Set<ISliceFilter> splitOrs = splitOr(Iterables.getOnlyElement(splitAnds));

				if (splitOrs.isEmpty()) {
					return IValueMatcher.MATCH_NONE;
				} else if (splitOrs.size() == 1) {
					throw new UnsupportedOperationException("filter:%s column:%s is not managed"
							.formatted(PepperLogHelper.getObjectAndClass(filter), column));
				} else {
					Set<IValueMatcher> orMatchers = splitOrs.stream()
							.map(f -> getValueMatcherLax(f, column, throwOnOr))
							// .filter(f -> !IValueMatcher.MATCH_ALL.equals(f))
							.collect(ImmutableSet.toImmutableSet());

					if (orMatchers.size() == 1) {
						// This is a common factor to all OR operands
						return Iterables.getOnlyElement(orMatchers);
					} else if (throwOnOr) {
						throw new UnsupportedOperationException("filter:%s column:%s is not managed"
								.formatted(PepperLogHelper.getObjectAndClass(filter), column));
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
		if (slice instanceof FlatAndFilter simpleAnd) {
			// Fast-path: extract raw values from the column→matcher map
			Map<String, Object> result = new LinkedHashMap<>(simpleAnd.columnToMatcher.size());
			simpleAnd.columnToMatcher.forEach((col, matcher) -> result.put(col, extractValue(matcher)));
			return result;
		} else if (slice.isMatchAll()) {
			return ImmutableMap.of();
		} else if (slice.isColumnFilter() && slice instanceof IColumnFilter columnFilter) {
			IValueMatcher valueMatcher = columnFilter.getValueMatcher();
			if (valueMatcher instanceof EqualsMatcher equalsMatcher) {
				return ImmutableMap.of(columnFilter.getColumn(), equalsMatcher.getWrapped());
			} else {
				throw new NotYetImplementedException("filter=%s".formatted(slice));
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
			throw new NotYetImplementedException("filter=%s".formatted(slice));
		}
	}

	/**
	 * Extracts the raw value from an {@link IValueMatcher} that represents a single equality condition.
	 * <ul>
	 * <li>{@link EqualsMatcher} → its operand</li>
	 * <li>{@link NullMatcher} → {@code null}</li>
	 * </ul>
	 *
	 * @throws eu.solven.adhoc.util.NotYetImplementedException
	 *             for matchers that cannot be represented as a single value
	 */
	protected static Object extractValue(IValueMatcher matcher) {
		if (matcher instanceof EqualsMatcher eq) {
			return eq.getWrapped();
		} else if (matcher instanceof NullMatcher) {
			return null;
		} else {
			throw new NotYetImplementedException("Cannot extract a raw value from matcher=%s".formatted(matcher));
		}
	}

	public static Set<String> getFilteredColumns(ISliceFilter filter) {
		if (filter instanceof IColumnFilter columnFilter) {
			// Fast-path to skip `Stream`
			return ImmutableSet.of(columnFilter.getColumn());
		}
		// Implementation rely on `.mapMulti` for better performance, not needed to create a `Stream.of` on
		// IColumnFilter
		return Stream.of(filter).mapMulti(FilterHelpers::emitFilteredColumns).collect(ImmutableSet.toImmutableSet());
	}

	// This has been coded with the help of ChatGPT...
	private static void emitFilteredColumns(ISliceFilter filter, Consumer<String> downstream) {
		if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter) {
			downstream.accept(columnFilter.getColumn());
		} else if (filter instanceof FlatAndFilter flat) {
			flat.columnToMatcher.keySet().forEach(downstream);
		} else if (filter instanceof IHasOperands<?> hasOperands) {
			hasOperands.getOperands().forEach(operand -> emitFilteredColumns((ISliceFilter) operand, downstream));
		} else if (filter.isNot() && filter instanceof INotFilter notFilter) {
			emitFilteredColumns(notFilter.getNegated(), downstream);
		} else {
			throw new NotYetImplementedException("filter=%s".formatted(PepperLogHelper.getObjectAndClass(filter)));
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

	@Deprecated(since = "Is this useful API?")
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
			throw new NotYetImplementedException("filter=%s".formatted(PepperLogHelper.getObjectAndClass(filter)));
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
		return new FilterUtility(AdhocFilterUnsafe.filterOptimizer).commonAnd(filters);
	}

	/**
	 * Given the input filters, considered to be `OR` together, this returns a `WHERE` filter, common to all input
	 * filters.
	 *
	 * @param filters
	 * @return
	 */
	public static ISliceFilter commonOr(ImmutableSet<? extends ISliceFilter> filters) {
		return new FilterUtility(AdhocFilterUnsafe.filterOptimizer).commonOr(filters);
	}

	/**
	 * Split the filter in a Set of {@link ISliceFilter}, equivalent by AND to the original filter.
	 *
	 * @param filter
	 * @return a Set of {@link ISliceFilter} equivalent with `AND`, guaranteed to be be themselves neither `AND` not
	 *         `NOT(OR(...))`
	 */
	public static Set<ISliceFilter> splitAnd(ISliceFilter filter) {
		return splitAnd(ImmutableSet.of(filter));
	}

	public static Set<ISliceFilter> splitAnd(Collection<? extends ISliceFilter> filters) {
		return splitAnd(filters, true);
	}

	public static Set<ISliceFilter> splitAnd(Collection<? extends ISliceFilter> filters, boolean splitMatchers) {
		Set<ISliceFilter> asSet = filters.stream()
				.<ISliceFilter>mapMulti((f, downstream) -> emitAndOperands(f, downstream, splitMatchers))
				// Skipping matchAll is useful on `.edit`
				.filter(f -> !f.isMatchAll())
				.collect(ImmutableSet.toImmutableSet());

		if (asSet.contains(ISliceFilter.MATCH_NONE)) {
			return ImmutableSet.of(ISliceFilter.MATCH_NONE);
		}

		return asSet;
	}

	/**
	 * 
	 * @param filter
	 * @param downstream
	 * @param splitMatchers
	 *            if true, {@link IValueMatcher} are also considered for being split by AND logic.
	 */
	// OPTIMIZATION: Flatten the whole input into a single Stream before collecting into a Set
	// OPTIMIZTION: mapMulti is faster (but more cumbersome) than flatMap
	public static void emitAndOperands(ISliceFilter filter, Consumer<ISliceFilter> downstream, boolean splitMatchers) {
		boolean emitted;
		if (filter instanceof FlatAndFilter flatAnd) {
			// Fast-path: iterate the backing column→matcher map directly, emitting ColumnFilter wrappers per entry.
			// Avoids the instanceof chain that the generic IAndFilter branch would run per operand.
			flatAnd.forEachOperand(downstream);
			emitted = true;
		} else if (filter instanceof IAndFilter andFilter) {
			andFilter.getOperands().forEach(o -> emitAndOperands(o, downstream, splitMatchers));
			emitted = true;
		} else if (filter instanceof INotFilter notFilter) {
			if (notFilter.getNegated() instanceof IOrFilter orFilter) {
				orFilter.getOperands()
						.stream()
						.map(ISliceFilter::negate)
						.forEach(o -> emitAndOperands(o, downstream, splitMatchers));
				emitted = true;
			} else if (splitMatchers && notFilter.getNegated() instanceof IColumnFilter columnFilter
					&& columnFilter.getValueMatcher() instanceof InMatcher inMatcher) {
				inMatcher.getOperands()
						.stream()
						.map(o -> ColumnFilter.match(columnFilter.getColumn(), NotMatcher.notEqualTo(o)))
						.forEach(o -> emitAndOperands(o, downstream, splitMatchers));
				emitted = true;
			} else {
				emitted = false;
			}
		} else if (splitMatchers && filter instanceof IColumnFilter columnFilter) {
			IValueMatcher valueMatcher = columnFilter.getValueMatcher();

			String column = columnFilter.getColumn();
			if (valueMatcher instanceof AndMatcher andMatcher) {
				andMatcher.getOperands()
						.stream()
						.map(operand -> ColumnFilter.builder().column(column).valueMatcher(operand).build())
						.forEach(o -> emitAndOperands(o, downstream, splitMatchers));
				emitted = true;
			} else if (valueMatcher instanceof NotMatcher notMatcher
					&& notMatcher.getNegated() instanceof InMatcher notInMatcher) {
				notInMatcher.getOperands()
						.stream()
						.map(operand -> ColumnFilter.builder()
								.column(column)
								.valueMatcher(NotMatcher.not(EqualsMatcher.matchEq(operand)))
								.build())
						.forEach(o -> emitAndOperands(o, downstream, splitMatchers));
				emitted = true;
			} else {
				emitted = false;
			}
		} else {
			emitted = false;
		}

		// Not splittable
		if (!emitted) {
			downstream.accept(filter);
		}
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
		return AdhocFilterUnsafe.filterStripperFactory.makeFilterStripper(stricter).isStricterThan(laxer);
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
		return stripWhereFromFilter(AdhocFilterUnsafe.filterStripperFactory, where, filter);
	}

	public static ISliceFilter stripWhereFromFilter(IFilterStripperFactory filterStripperFactory,
			ISliceFilter where,
			ISliceFilter filter) {
		return filterStripperFactory.makeFilterStripper(where).strip(filter);
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
		return simplifyOrGivenContribution(AdhocFilterUnsafe.filterStripperFactory, contribution, filter);
	}

	public static ISliceFilter simplifyOrGivenContribution(IFilterStripperFactory filterStripperFactory,
			ISliceFilter contribution,
			ISliceFilter filter) {
		return stripWhereFromFilter(filterStripperFactory, contribution.negate(), filter.negate()).negate();
	}

	/**
	 * This comparison enables a deterministic ordering of {@link ISliceFilter}. It does not implies anything in term of
	 * relative complexity.
	 * 
	 * @return a Comparator for {@link ISliceFilter}, enabling deterministic ordering.
	 */
	@Deprecated(since = "Not ready")
	public static Comparator<? super ISliceFilter> filterComparator() {
		return (l, r) -> l.toString().compareTo(r.toString());
	}

	/**
	 * Partitions {@code filters} into independent column clusters.
	 *
	 * <p>
	 * Two filters belong to the same cluster when their column sets (as returned by {@link #getFilteredColumns}) are
	 * not disjoint — i.e. they share at least one column. Filters in different clusters have fully disjoint column sets
	 * and therefore cannot interact logically; each cluster can be optimised independently.
	 *
	 * <p>
	 * The grouping is performed by a Union-Find over the distinct column-sets. Since the number of distinct column-sets
	 * is typically very small (low keySet cardinality), the O(k²) union step is acceptable.
	 *
	 * <p>
	 * When all filters share at least one column (single cluster), the method returns a singleton set containing the
	 * original input, without allocating any intermediate collection.
	 *
	 * @param filters
	 *            the filters to partition; must not be {@code null}
	 * @return a set of clusters, where each cluster is a non-empty set of filters whose column sets are connected;
	 *         never empty (a single-element result means no split was possible)
	 */
	public static Set<Set<? extends ISliceFilter>> clusterFilters(Set<? extends ISliceFilter> filters) {
		// Group by column-set. We expect very few distinct column-sets (low cardinality), so the
		// Multimap key-set is tiny. Filters that touch exactly the same columns share one bucket.
		SetMultimap<Set<String>, ISliceFilter> byColumnSet =
				MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();
		for (ISliceFilter f : filters) {
			byColumnSet.put(getFilteredColumns(f), f);
		}

		int keySetCardinality = byColumnSet.keySet().size();
		if (keySetCardinality <= 1) {
			// fast-path for many cases
			return ImmutableSet.of(filters);
		}

		List<Set<String>> keys = new ArrayList<>(byColumnSet.keySet());

		// Union-Find over the k distinct column-sets (k is small, O(k²) is acceptable).
		int[] parent = new int[keySetCardinality];
		for (int i = 0; i < keySetCardinality; i++) {
			parent[i] = i;
		}
		// Evaluate each keySet against all other keySet: O(n*2)/2
		for (int i = 0; i < keySetCardinality; i++) {
			Set<String> ki = keys.get(i);

			for (int j = i + 1; j < keySetCardinality; j++) {
				if (!Collections.disjoint(ki, keys.get(j))) {
					int ri = findRoot(parent, i);
					int rj = findRoot(parent, j);
					if (ri != rj) {
						parent[ri] = rj;
					}
				}
			}
		}

		// Collect filters into their cluster, preserving the original per-key order.
		Map<Integer, Set<ISliceFilter>> clusters = new LinkedHashMap<>();
		for (int i = 0; i < keySetCardinality; i++) {
			clusters.computeIfAbsent(findRoot(parent, i), x -> new LinkedHashSet<>())
					.addAll(byColumnSet.get(keys.get(i)));
		}

		return ImmutableSet.copyOf(clusters.values());
	}

	protected static int findRoot(int[] parent, int i) {
		while (parent[i] != i) {
			parent[i] = parent[parent[i]]; // Path compression (two-step)
			i = parent[i];
		}
		return i;
	}

}

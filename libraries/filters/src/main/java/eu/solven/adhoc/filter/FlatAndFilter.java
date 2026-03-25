/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.filter.value.IValueMatcher;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

/**
 * A compact {@link IAndFilter} backed by an {@link ImmutableMap} from column name to {@link IValueMatcher}.
 *
 * <p>
 * Each column condition may be any {@link IValueMatcher} — equality, null, in-set, etc. The factory {@link #of(Map)}
 * normalises raw values via {@link IValueMatcher#matching(Object)}, so callers can pass plain values, {@code null},
 * {@link java.util.Collection}s, or ready-made matchers interchangeably.
 *
 * <p>
 * Unlike {@link AndFilter}, operands are guaranteed to be flat per-column {@link IValueMatcher} conditions — there are
 * no recursive {@link ISliceFilter} children. This makes it the natural representation for slice-derived filters.
 *
 * <p>
 * {@link #getOperands()} materialises {@link ColumnFilter} wrappers on demand from the backing map.
 *
 * <p>
 * This is the type returned by {@link eu.solven.adhoc.cuboid.slice.ISlice#asFilter()}, making it the natural fast-path
 * for tight loops that start from a slice and immediately call {@link FilterHelpers#splitAnd(java.util.Collection)}.
 *
 * <p>
 * <b>Equality and hashing</b>: {@link #equals(Object)} is intentionally cross-type — a {@code FlatAndFilter} compares
 * equal to any {@link IAndFilter} whose operands are all single-column {@link IValueMatcher} filters whose
 * column→matcher pairs match exactly. {@link #hashCode()} follows the same {@link java.util.Set}-based contract as
 * {@link AndFilter}, so semantically equivalent instances hash to the same bucket. Note: the reverse direction of
 * {@code equals} ({@link AndFilter#equals(Object)} accepting a {@code FlatAndFilter}) is <em>not</em> guaranteed
 * because {@link AndFilter} uses a Lombok-generated {@code @Value} equals.
 *
 * <p>
 * <b>JSON</b>: serialised identically to an {@link AndFilter} so existing consumers see no change.
 *
 * @author Benoit Lacelle
 * @see AndFilter#and(Map) for the general-purpose factory that handles arbitrary
 *      {@link eu.solven.adhoc.filter.value.IValueMatcher}s
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class FlatAndFilter implements IAndFilter {

	final ImmutableMap<String, IValueMatcher> columnToMatcher;

	/**
	 * Smart factory: returns the most specific {@link ISliceFilter} for the given column→value map.
	 * <ul>
	 * <li>Empty map → {@link ISliceFilter#MATCH_ALL}</li>
	 * <li>Single entry → {@link ColumnFilter#matchLax(String, Object)}</li>
	 * <li>Two or more entries → a {@link FlatAndFilter}</li>
	 * </ul>
	 * Values are normalised via {@link IValueMatcher#matching(Object)}: {@code null} becomes
	 * {@link eu.solven.adhoc.filter.value.NullMatcher}, a {@link java.util.Collection} becomes
	 * {@link eu.solven.adhoc.filter.value.InMatcher}, a ready-made {@link IValueMatcher} is used as-is, and any other
	 * value becomes an {@link eu.solven.adhoc.filter.value.EqualsMatcher}.
	 *
	 * @param columnToValue
	 *            the conditions, keyed by column name
	 * @return the most specific {@link ISliceFilter} for the given map
	 */
	public static ISliceFilter of(Map<String, ?> columnToValue) {
		if (columnToValue.isEmpty()) {
			return MATCH_ALL;
		} else if (columnToValue.size() == 1) {
			Map.Entry<String, ?> entry = columnToValue.entrySet().iterator().next();
			return ColumnFilter.matchLax(entry.getKey(), entry.getValue());
		} else {
			ImmutableMap.Builder<String, IValueMatcher> builder =
					ImmutableMap.builderWithExpectedSize(columnToValue.size());
			columnToValue.forEach((k, v) -> builder.put(k, IValueMatcher.matching(v)));
			return new FlatAndFilter(builder.build());
		}
	}

	@Override
	public boolean isMatchAll() {
		return columnToMatcher.isEmpty();
	}

	@Override
	public boolean isMatchNone() {
		return false;
	}

	@Override
	public boolean isAnd() {
		return true;
	}

	/**
	 * Materialises {@link ColumnFilter} wrappers on demand. Prefer {@link FilterHelpers#emitAndOperands} fast-path when
	 * iterating, which avoids this allocation.
	 */
	@Override
	public Set<ISliceFilter> getOperands() {
		return columnToMatcher.entrySet()
				.stream()
				.map(e -> ColumnFilter.builder().column(e.getKey()).valueMatcher(e.getValue()).build())
				.collect(ImmutableSet.toImmutableSet());
	}

	@Override
	public ISliceFilter negate() {
		// A simple `NOT` to prevent having to `.negate` all operands
		return FilterBuilder.not(this).combine();
	}

	// -------------------------------------------------------------------------
	// equals / hashCode
	// -------------------------------------------------------------------------

	/**
	 * Cross-type equality: returns {@code true} when compared to any {@link IAndFilter} that represents exactly the
	 * same column = value constraints.
	 *
	 * <p>
	 * NOTE: {@link AndFilter#equals(Object)} does not reciprocate for {@code FlatAndFilter} arguments (it is generated
	 * by {@code @Value}). Assertions where this instance is the {@code actual} value work correctly; assertions where
	 * it is the {@code expected} value against an {@link AndFilter} actual may not.
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		} else if (o instanceof FlatAndFilter other) {
			return columnToMatcher.equals(other.columnToMatcher);
		} else if (o instanceof IAndFilter andFilter) {
			Set<ISliceFilter> operands = andFilter.getOperands();
			if (operands.size() != columnToMatcher.size()) {
				return false;
			}
			for (ISliceFilter operand : operands) {
				if (!(operand instanceof IColumnFilter columnFilter)) {
					return false;
				}
				IValueMatcher thisMatcher = columnToMatcher.get(columnFilter.getColumn());
				if (thisMatcher == null || !thisMatcher.equals(columnFilter.getValueMatcher())) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * Hash compatible with {@link AndFilter#hashCode()}: delegates to {@link #getOperands()}{@code .hashCode()}, which
	 * follows the standard {@link java.util.Set} contract (sum of element hash codes).
	 */
	@Override
	public int hashCode() {
		return getOperands().hashCode();
	}

	// -------------------------------------------------------------------------
	// toString
	// -------------------------------------------------------------------------

	@Override
	public String toString() {
		if (isMatchAll()) {
			return "matchAll";
		}
		return columnToMatcher.entrySet()
				.stream()
				.map(e -> ColumnFilter.match(e.getKey(), e.getValue()))
				.map(ISliceFilter::toString)
				.collect(Collectors.joining("&"));
	}

}

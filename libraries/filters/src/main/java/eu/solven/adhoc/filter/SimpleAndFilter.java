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
import com.google.common.collect.Iterables;

import eu.solven.adhoc.filter.value.EqualsMatcher;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

/**
 * A compact {@link IAndFilter} restricted to an AND of per-column {@link EqualsMatcher} conditions.
 *
 * <p>
 * Backed by an {@link ImmutableMap} rather than a {@link Set} of {@link ColumnFilter} wrappers, so construction from a
 * slice or a plain {@link Map} allocates no per-entry objects. {@link #getOperands()} materialises {@link ColumnFilter}
 * instances lazily, on first call.
 *
 * <p>
 * This is the type returned by {@link eu.solven.adhoc.cuboid.slice.ISlice#asFilter()}, making it the natural fast-path
 * for tight loops that start from a slice and immediately call {@link FilterHelpers#splitAnd(java.util.Collection)}.
 *
 * <p>
 * <b>Equality and hashing</b>: {@link #equals(Object)} is intentionally cross-type — a {@code SimpleAndFilter} compares
 * equal to any {@link IAndFilter} whose operands are all single-column {@link EqualsMatcher} filters that cover exactly
 * the same column→value pairs. {@link #hashCode()} follows the same {@link java.util.Set}-based contract as
 * {@link AndFilter}, so semantically equivalent instances hash to the same bucket. Note: the reverse direction of
 * {@code equals} ({@link AndFilter#equals(Object)} accepting a {@code SimpleAndFilter}) is <em>not</em> guaranteed
 * because {@link AndFilter} uses a Lombok-generated {@code @Value} equals.
 *
 * <p>
 * <b>JSON</b>: serialised identically to an {@link AndFilter} so existing consumers see no change.
 *
 * @author Benoit Lacelle
 * @see AndFilter#and(Map) for the general-purpose factory that handles non-equality matchers
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class SimpleAndFilter implements IAndFilter {

	// null values are not stored here: of() redirects null-containing maps to AndFilter.and()
	final ImmutableMap<String, Object> columnToValue;

	/**
	 * Smart factory: returns the most specific {@link ISliceFilter} for the given column→value map.
	 * <ul>
	 * <li>Empty map → {@link ISliceFilter#MATCH_ALL}</li>
	 * <li>Single entry → {@link ColumnFilter#matchEq(String, Object)}</li>
	 * <li>Two or more entries → a {@link SimpleAndFilter}</li>
	 * </ul>
	 * Values must already be normalised equality operands (no {@link eu.solven.adhoc.filter.value.IValueMatcher}, no
	 * {@link java.util.Collection}, no {@code null}).
	 *
	 * @param columnToValue
	 *            the equality conditions, keyed by column name
	 * @return the most specific {@link ISliceFilter} for the given map
	 */
	public static ISliceFilter of(Map<String, ?> columnToValue) {
		if (columnToValue.isEmpty()) {
			return MATCH_ALL;
		} else if (columnToValue.size() == 1) {
			Map.Entry<String, ?> entry = columnToValue.entrySet().iterator().next();
			return ColumnFilter.matchEq(entry.getKey(), entry.getValue());
		} else if (Iterables.contains(columnToValue.values(), null)) {
			// Null values cannot be represented as EqualsMatcher; delegate to AndFilter which maps null → NullMatcher
			return AndFilter.and(columnToValue);
		} else {
			return new SimpleAndFilter(ImmutableMap.copyOf(columnToValue));
		}
	}

	@Override
	public boolean isMatchAll() {
		return columnToValue.isEmpty();
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
		return columnToValue.entrySet()
				.stream()
				.map(e -> ColumnFilter.builder().column(e.getKey()).matchEquals(e.getValue()).build())
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
	 * NOTE: {@link AndFilter#equals(Object)} does not reciprocate for {@code SimpleAndFilter} arguments (it is
	 * generated by {@code @Value}). Assertions where this instance is the {@code actual} value work correctly;
	 * assertions where it is the {@code expected} value against an {@link AndFilter} actual may not.
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		} else if (o instanceof SimpleAndFilter other) {
			return columnToValue.equals(other.columnToValue);
		} else if (o instanceof IAndFilter andFilter) {
			Set<ISliceFilter> operands = andFilter.getOperands();
			if (operands.size() != columnToValue.size()) {
				return false;
			}
			for (ISliceFilter operand : operands) {
				if (!(operand instanceof IColumnFilter columnFilter)) {
					return false;
				} else if (!(columnFilter.getValueMatcher() instanceof EqualsMatcher equalsMatcher)) {
					return false;
				} else {
					Object thisValue = columnToValue.get(columnFilter.getColumn());
					if (thisValue == null) {
						return false;
					} else if (!equalsMatcher.match(thisValue)) {
						// Use .match() to respect EqualsMatcher's numeric-normalisation rules
						return false;
					}
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
		return columnToValue.entrySet()
				.stream()
				.map(e -> "%s==%s".formatted(e.getKey(), e.getValue()))
				.collect(Collectors.joining("&"));
	}

}

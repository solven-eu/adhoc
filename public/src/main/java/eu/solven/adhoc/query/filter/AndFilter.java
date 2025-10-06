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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import eu.solven.adhoc.query.filter.value.IValueMatcher;
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

	@JsonProperty("filters")
	@Singular
	@NonNull
	final ImmutableSet<ISliceFilter> ands;

	// This constructor helps not copying ImmutableSet, as `@Singular` always generates a builder, preventing `.copyOf`
	// optimization
	@Deprecated(since = "Legit API?")
	AndFilter(Collection<? extends ISliceFilter> ands) {
		this.ands = ImmutableSet.copyOf(ands);
	}

	@Override
	public boolean isNot() {
		return false;
	}

	@Override
	public boolean isMatchAll() {
		// An empty AND is considered to match everything
		return ands.isEmpty();
	}

	@Override
	public boolean isMatchNone() {
		return ands.stream().anyMatch(ISliceFilter::isMatchNone);
	}

	@Override
	public boolean isAnd() {
		return true;
	}

	@Override
	public Set<ISliceFilter> getOperands() {
		return ImmutableSet.copyOf(ands);
	}

	@Override
	public String toString() {
		if (isMatchAll()) {
			return "matchAll";
		}

		int size = ands.size();
		if (size <= AdhocUnsafe.limitOrdinalToString) {
			return ands.stream().map(o -> {
				if (o instanceof OrFilter orFilter) {
					return "(%s)".formatted(orFilter);
				} else {
					return o.toString();
				}
			}).collect(Collectors.joining("&"));
		} else {
			ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", size);

			AtomicInteger index = new AtomicInteger();
			ands.stream().limit(AdhocUnsafe.limitOrdinalToString).forEach(filter -> {
				toStringHelper.add("#" + index.getAndIncrement(), filter);
			});

			return toStringHelper.toString();
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
	public static ISliceFilter and(Map<String, ?> columnToFilter) {
		int size = columnToFilter.size();
		List<ColumnFilter> columnFilters = new ArrayList<>(size);

		columnToFilter.forEach((k, v) -> {
			columnFilters.add(ColumnFilter.builder().column(k).matching(v).build());
		});

		// BEWARE Do not call `.and` due to most optimizations/checks are irrelevant
		// And this is a performance bottleneck in Shiftor
		if (columnFilters.isEmpty()) {
			return MATCH_ALL;
		} else if (columnFilters.size() == 1) {
			return columnFilters.getFirst();
		} else {
			return builder().ands(columnFilters).build();
		}
	}

	@Deprecated(since = "FilterBuilder.and")
	public static ISliceFilter and(ISliceFilter first, ISliceFilter second, ISliceFilter... more) {
		return FilterBuilder.and(Lists.asList(first, second, more)).optimize();
	}

	@Deprecated(since = "FilterBuilder.and")
	public static ISliceFilter and(Collection<? extends ISliceFilter> filters) {
		return FilterBuilder.and(filters).optimize();
	}

	@Override
	public ISliceFilter negate() {
		return FilterBuilder.or(getOperands().stream().map(ISliceFilter::negate).toList()).combine();
	}

}
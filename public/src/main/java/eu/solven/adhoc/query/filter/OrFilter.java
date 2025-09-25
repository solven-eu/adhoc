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
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Default implementation for {@link IOrFilter}.
 *
 * Prefer `.or(...)` to optimize the matcher, except if you need an unoptimized `OrFilter`.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
@Jacksonized
public class OrFilter implements IOrFilter {

	@JsonProperty("filters")
	@Singular
	@NonNull
	final ImmutableSet<ISliceFilter> ors;

	@Override
	public boolean isNot() {
		return false;
	}

	@Override
	public boolean isMatchNone() {
		// An empty OR is considered to match nothing
		return ors.isEmpty();
	}

	@Override
	public boolean isMatchAll() {
		return ors.stream().anyMatch(ISliceFilter::isMatchAll);
	}

	@Override
	public boolean isOr() {
		return true;
	}

	@Override
	public Set<ISliceFilter> getOperands() {
		return ors;
	}

	@Override
	public ISliceFilter negate() {
		return FilterBuilder.and(getOperands().stream().map(NotFilter::not).toList()).combine();
	}

	@Override
	public String toString() {
		if (isMatchNone()) {
			return "matchNone";
		}

		int size = ors.size();
		if (size <= AdhocUnsafe.limitOrdinalToString) {
			return ors.stream().map(Object::toString).collect(Collectors.joining("|"));
		} else {
			MoreObjects.ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", ors.size());

			AtomicInteger index = new AtomicInteger();
			ors.stream().limit(AdhocUnsafe.limitOrdinalToString).forEach(filter -> {
				toStringHelper.add("#" + index.getAndIncrement(), filter);
			});

			return toStringHelper.toString();
		}
	}

	/**
	 *
	 * @param columnToFilter
	 *            each key maps to a column, while each value represent a matcher. May be an {@link IValueMatcher}, or a
	 *            {@link Collection}, a `null`, or a literal.
	 *
	 * @return a filter doing an `OR` between each {@link Map} entry,
	 */
	public static ISliceFilter or(Map<String, ?> columnToFilter) {
		List<? extends ISliceFilter> asList = columnToFilter.entrySet()
				.stream()
				.map(e -> ColumnFilter.matchLax(e.getKey(), e.getValue()))
				.collect(Collectors.toCollection(ArrayList::new));

		if (asList.contains(MATCH_ALL)) {
			return MATCH_ALL;
		}

		asList.removeIf(MATCH_NONE::equals);

		return FilterBuilder.or(asList).combine();
	}
}
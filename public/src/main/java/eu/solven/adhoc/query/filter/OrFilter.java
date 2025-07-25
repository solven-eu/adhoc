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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

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

	@Singular
	@NonNull
	final ImmutableSet<IAdhocFilter> filters;

	@Override
	public boolean isNot() {
		return false;
	}

	@Override
	public boolean isMatchNone() {
		// An empty OR is considered to match nothing
		return filters.isEmpty();
	}

	@Override
	public boolean isMatchAll() {
		return filters.stream().anyMatch(IAdhocFilter::isMatchAll);
	}

	@Override
	public boolean isOr() {
		return true;
	}

	@Override
	public Set<IAdhocFilter> getOperands() {
		return filters;
	}

	@Override
	public String toString() {
		if (isMatchNone()) {
			return "matchNone";
		}

		int size = filters.size();
		if (size <= AdhocUnsafe.limitOrdinalToString) {
			return filters.stream().map(Object::toString).collect(Collectors.joining("|"));
		} else {
			MoreObjects.ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", filters.size());

			AtomicInteger index = new AtomicInteger();
			filters.stream().limit(AdhocUnsafe.limitOrdinalToString).forEach(filter -> {
				toStringHelper.add("#" + index.getAndIncrement(), filter);
			});

			return toStringHelper.toString();
		}
	}

	public static IAdhocFilter or(Collection<? extends IAdhocFilter> filters) {
		if (filters.stream().anyMatch(IAdhocFilter::isMatchAll)) {
			return MATCH_ALL;
		}

		List<? extends IAdhocFilter> notMatchNone = filters.stream().filter(f -> !f.isMatchNone()).flatMap(operand -> {
			if (operand instanceof IOrFilter operandIsOr) {
				// OR of ORs
				return operandIsOr.getOperands().stream();
			} else {
				return Stream.of(operand);
			}
		}).collect(Collectors.toList());

		if (notMatchNone.isEmpty()) {
			return MATCH_NONE;
		} else if (notMatchNone.size() == 1) {
			return notMatchNone.getFirst();
		} else {
			// TODO Rely on `Not` and `And` for optimizations
			return OrFilter.builder().filters(notMatchNone).build();
		}
	}

	// `first, second, more` syntax to push providing at least 2 arguments
	public static IAdhocFilter or(IAdhocFilter first, IAdhocFilter second, IAdhocFilter... more) {
		return or(Lists.asList(first, second, more));
	}

	/**
	 *
	 * @param columnToFilter
	 *            each key maps to a column, while each value represent a matcher. May be an {@link IValueMatcher}, or a
	 *            {@link Collection}, a `null`, or a literal.
	 *
	 * @return a filter doing an `OR` between each {@link Map} entry,
	 */
	public static IAdhocFilter or(Map<String, ?> columnToFilter) {
		return or(columnToFilter.entrySet()
				.stream()
				.map(e -> ColumnFilter.builder().column(e.getKey()).matching(e.getValue()).build())
				.collect(Collectors.toList()));
	}
}
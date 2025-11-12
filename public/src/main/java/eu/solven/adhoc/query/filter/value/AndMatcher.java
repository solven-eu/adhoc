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
package eu.solven.adhoc.query.filter.value;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IHasOperands;
import eu.solven.adhoc.util.AdhocCollectionHelpers;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * To be used with {@link ColumnFilter}, for AND matchers. True if there is not a single operand.
 *
 * Prefer `.and(...)` to optimize the matcher, except if you need an unoptimized `AndMatcher`.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
@Jacksonized
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class AndMatcher implements IValueMatcher, IHasOperands<IValueMatcher> {

	// @JsonProperty("matchers")
	@JsonProperty("operands")
	@NonNull
	@Singular
	ImmutableSet<IValueMatcher> ands;

	public static IValueMatcher copyOf(Collection<? extends IValueMatcher> ands) {
		return new AndMatcher(ImmutableSet.copyOf(ands));
	}

	@JsonIgnore
	@Override
	public Set<IValueMatcher> getOperands() {
		return getAnds();
	}

	@JsonIgnore
	public boolean isMatchAll() {
		return ands.isEmpty();
	}

	@Override
	public String toString() {
		if (isMatchAll()) {
			return "matchAll";
		}

		int size = ands.size();
		if (size <= AdhocUnsafe.getLimitOrdinalToString()) {
			return ands.stream().map(o -> {
				if (o instanceof OrMatcher orMatcher) {
					// Add parenthesis
					return "(%s)".formatted(orMatcher);
				} else {
					return o.toString();
				}
			}).collect(Collectors.joining("&"));
		} else {
			MoreObjects.ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", size);

			AtomicInteger index = new AtomicInteger();
			ands.stream().limit(AdhocUnsafe.getLimitOrdinalToString()).forEach(filter -> {
				toStringHelper.add("#" + index.getAndIncrement(), filter);
			});

			return toStringHelper.toString();
		}
	}

	@Override
	public boolean match(Object value) {
		return ands.stream().allMatch(operand -> operand.match(value));
	}

	// `first, second, more` syntax to push providing at least 2 arguments
	public static IValueMatcher and(IValueMatcher first, IValueMatcher second, IValueMatcher... more) {
		return and(Lists.asList(first, second, more));
	}

	public static IValueMatcher and(Collection<? extends IValueMatcher> filters) {
		return and(filters, true);
	}

	public static IValueMatcher and(Collection<? extends IValueMatcher> filters, boolean doSimplify) {
		if (filters.contains(MATCH_NONE)) {
			return MATCH_NONE;
		}

		// AND of ANDs
		List<? extends IValueMatcher> notMatchAll = AdhocCollectionHelpers.unnestAsList(AndMatcher.class,
				AndMatcher.builder().ands(filters).build(),
				f -> f.getOperands().size(),
				AndMatcher::getOperands,
				// Skipping matchAll is useful on `.edit`
				f -> !MATCH_ALL.equals(f));

		if (doSimplify) {
			notMatchAll = MatcherOptimizerHelpers.simplifiedOwned(notMatchAll);
		}

		if (notMatchAll.isEmpty()) {
			return MATCH_ALL;
		} else if (notMatchAll.size() == 1) {
			return notMatchAll.getFirst();
		} else {
			return AndMatcher.builder().ands(notMatchAll).build();
		}
	}
}

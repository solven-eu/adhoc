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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IHasOperands;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.util.AdhocCollectionHelpers;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * To be used with {@link ColumnFilter}, for OR matchers. False if not a single operand.
 *
 * Prefer `.or(...)` to optimize the matcher, except if you need an unoptimized `OrMatcher`.
 *
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class OrMatcher implements IValueMatcher, IHasOperands<IValueMatcher> {

	// @JsonProperty("matchers")
	@JsonProperty("operands")
	@NonNull
	@Singular
	ImmutableSet<IValueMatcher> ors;

	public static IValueMatcher copyOf(Collection<? extends IValueMatcher> ors) {
		return new OrMatcher(ImmutableSet.copyOf(ors));
	}

	@JsonIgnore
	@Override
	public Set<IValueMatcher> getOperands() {
		return getOrs();
	}

	@JsonIgnore
	public boolean isMatchNone() {
		return ors.isEmpty();
	}

	@Override
	public boolean match(Object value) {
		return ors.stream().anyMatch(operand -> operand.match(value));
	}

	@Override
	public String toString() {
		if (isMatchNone()) {
			return "matchNone";
		}

		return OrFilter.toString2(this, ors);
	}

	// `first, second, more` syntax to push providing at least 2 arguments
	public static IValueMatcher or(IValueMatcher first, IValueMatcher second, IValueMatcher... more) {
		return or(Lists.asList(first, second, more));
	}

	public static IValueMatcher or(Collection<? extends IValueMatcher> filters) {
		return or(filters, true);
	}

	public static IValueMatcher or(Collection<? extends IValueMatcher> filters, boolean doSimplify) {
		if (filters.contains(MATCH_ALL)) {
			return MATCH_ALL;
		}

		// OR of ORs
		List<? extends IValueMatcher> notMatchNone = AdhocCollectionHelpers.unnestAsList(OrMatcher.class,
				OrMatcher.builder().ors(filters).build(),
				f -> f.getOperands().size(),
				OrMatcher::getOperands,
				// Skipping matchAll is useful on `.edit`
				f -> !MATCH_NONE.equals(f));

		if (doSimplify) {
			return simplifiedOwned(notMatchNone);
		} else {
			if (notMatchNone.isEmpty()) {
				return MATCH_NONE;
			} else if (notMatchNone.size() == 1) {
				return notMatchNone.getFirst();
			} else {
				return OrMatcher.builder().ors(notMatchNone).build();
			}
		}
	}

	/**
	 * This will `not` the input OR into an `AND`, so that only `AND` has complex/rich simplification rules
	 * 
	 * @param matchers
	 * @return
	 */
	private static IValueMatcher simplifiedOwned(List<? extends IValueMatcher> matchers) {
		if (matchers.isEmpty()) {
			return MATCH_NONE;
		} else if (matchers.size() == 1) {
			// InMatcher of simple operands would lead to OrMatchger with a single operand
			return matchers.getFirst();
		}

		List<IValueMatcher> negated = new ArrayList<>();
		matchers.forEach(matcher -> negated.add(NotMatcher.not(matcher)));

		// `a OR b OR c` is `!(!a AND !b AND !c)`
		IValueMatcher simplified = AndMatcher.and(negated);
		// `WithoutSimplifications` to prevent infinite cycle
		return NotMatcher.not(simplified, false);
	}

}

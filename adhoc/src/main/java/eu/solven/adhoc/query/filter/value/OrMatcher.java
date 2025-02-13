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
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.MoreObjects;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IHasOperands;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * To be used with {@link ColumnFilter}, for OR matchers. False if not a single operand.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
public class OrMatcher implements IValueMatcher, IHasOperands<IValueMatcher> {
	@NonNull
	@Singular
	ImmutableSet<IValueMatcher> operands;

	@JsonIgnore
	public boolean isMatchNone() {
		return operands.isEmpty();
	}

	@Override
	public boolean match(Object value) {
		return operands.stream().anyMatch(operand -> operand.match(value));
	}

	@Override
	public String toString() {
		if (isMatchNone()) {
			return "matchNone";
		}

		int size = operands.size();
		if (size <= AdhocUnsafe.limitOrdinalToString) {
			return operands.stream().map(Object::toString).collect(Collectors.joining("|"));
		} else {
			MoreObjects.ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", operands.size());

			AtomicInteger index = new AtomicInteger();
			operands.stream().limit(AdhocUnsafe.limitOrdinalToString).forEach(filter -> {
				toStringHelper.add("#" + index.getAndIncrement(), filter);
			});

			return toStringHelper.toString();
		}
	}

	public static IValueMatcher or(IValueMatcher first, IValueMatcher second, IValueMatcher... more) {
		return or(Lists.asList(first, second, more));
	}

	public static IValueMatcher or(Collection<? extends IValueMatcher> filters) {
		if (filters.stream().anyMatch(f -> f instanceof AndMatcher andMatcher && andMatcher.isMatchAll())) {
			return MATCH_ALL;
		}

		// Skipping matchAll is useful on `.edit`
		List<? extends IValueMatcher> notMatchNone = filters.stream()
				.filter(f -> !(f instanceof OrMatcher orMatcher && orMatcher.isMatchNone()))
				.flatMap(operand -> {
					if (operand instanceof OrMatcher operandIsOr) {
						// OR of ORs
						return operandIsOr.getOperands().stream();
					} else {
						return Stream.of(operand);
					}
				})
				.collect(Collectors.toList());

		if (notMatchNone.isEmpty()) {
			return IValueMatcher.MATCH_NONE;
		} else if (notMatchNone.size() == 1) {
			return notMatchNone.getFirst();
		} else {
			return OrMatcher.builder().operands(notMatchNone).build();
		}
	}
}

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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * To be used with {@link ColumnFilter}, for IN-based matchers.
 *
 * Prefer `.in(...)` to optimize the matcher, except if you need an unoptimized `InMatcher`.
 *
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
public class InMatcher implements IValueMatcher, IColumnToString {
	@NonNull
	@Singular
	ImmutableSet<?> operands;

	@Override
	public boolean match(Object value) {
		if (operands.contains(value)) {
			return true;
		}

		if (operands.stream().anyMatch(operand -> operand instanceof IValueMatcher vm && vm.match(value))) {
			return true;
		}

		return false;
	}

	@Override
	public String toString() {
		MoreObjects.ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", operands.size());

		AtomicInteger index = new AtomicInteger();
		operands.stream().limit(AdhocUnsafe.limitOrdinalToString).forEach(filter -> {
			toStringHelper.add("#" + index.getAndIncrement(), filter);
		});

		return toStringHelper.toString();
	}

	/**
	 *
	 * @param allowedValues
	 *            {@link Collection} will be unnested. `null` and `IValueMatchers` are managed specifically.
	 * @return
	 */
	@SuppressWarnings("PMD.LinguisticNaming")
	public static IValueMatcher isIn(Collection<?> allowedValues) {
		// TODO Unnest recursively
		List<Object> unnested = allowedValues.stream().flatMap(allowed -> {
			if (allowed instanceof Collection<?> asCollection) {
				return asCollection.stream();
			} else {
				// `allowed` may be null
				return Stream.of(allowed);
			}
		}).toList();

		if (unnested.isEmpty()) {
			return MATCH_NONE;
		} else if (unnested.size() == 1) {
			Object singleValue = unnested.getFirst();
			return EqualsMatcher.isEqualTo(singleValue);
		}

		boolean hasNull = unnested.contains(null);

		Map<Boolean, List<Object>> byValueMatcher = unnested.stream()
				.filter(Objects::nonNull)
				.collect(Collectors.partitioningBy(operand -> operand instanceof IValueMatcher));

		List<Object> unnestedNotNull = byValueMatcher.getOrDefault(Boolean.FALSE, List.of());
		List<IValueMatcher> valueMatchers = (List) byValueMatcher.getOrDefault(Boolean.TRUE, List.of());

		List<IValueMatcher> orOperands = new ArrayList<>();

		if (hasNull) {
			orOperands.add(NullMatcher.matchNull());
		}
		orOperands.addAll(valueMatchers);
		if (!unnestedNotNull.isEmpty()) {
			orOperands.add(InMatcher.builder().operands(unnestedNotNull).build());
		}

		return OrMatcher.or(orOperands);
	}

	/**
	 * {@link Collection} will be unnested.
	 * 
	 * @return
	 */
	@SuppressWarnings("PMD.LinguisticNaming")
	public static IValueMatcher isIn(Object first, Object second, Object... more) {
		return isIn(Lists.asList(first, second, more));
	}

	/**
	 *
	 * @param valueMatcher
	 *            some {@link IValueMatcher} expected to be an {@link EqualsMatcher} or an InMatcher
	 * @param clazz
	 *            the expected type of the operands
	 * @return the {@link Set} of operands specifically defined by the IValueMatcher, assignable to clazz.
	 * @param <T>
	 */
	public static <T> Set<T> extractOperands(IValueMatcher valueMatcher, Class<T> clazz) {
		if (valueMatcher instanceof EqualsMatcher equalsMatcher) {
			return EqualsMatcher.extractOperand(equalsMatcher, clazz).map(ImmutableSet::of).orElse(ImmutableSet.of());
		} else if (valueMatcher instanceof InMatcher inMatcher) {
			ImmutableSet<?> operands = inMatcher.getOperands();

			return operands.stream().filter(clazz::isInstance).map(clazz::cast).collect(ImmutableSet.toImmutableSet());
		} else {
			return ImmutableSet.of();
		}
	}

	@Override
	public String toString(String column, boolean negated) {
		// https://github.com/jirutka/rsql-parser?tab=readme-ov-file#grammar-and-semantic
		String operandsToString = operands.stream().map(String::valueOf).collect(Collectors.joining(",", "(", ")"));

		if (negated) {
			return column + "=out=" + operandsToString;
		} else{
			return column + "=in=" + operandsToString;
		}
	}
}

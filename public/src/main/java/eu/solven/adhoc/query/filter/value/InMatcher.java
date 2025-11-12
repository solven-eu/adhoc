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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.util.AdhocCollectionHelpers;
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
public final class InMatcher implements IValueMatcher, IColumnToString {
	@NonNull
	@Singular
	ImmutableSet<?> operands;

	@Builder
	@Jacksonized
	private InMatcher(Collection<?> operands) {
		this.operands = AdhocPrimitiveHelpers.normalizeValues(operands);
	}

	@Override
	public boolean match(Object value) {
		Object normalizedValue = AdhocPrimitiveHelpers.normalizeValue(value);

		if (operands.contains(normalizedValue)) {
			return true;
		}

		if (operands.stream().anyMatch(operand -> operand instanceof IValueMatcher vm && vm.match(normalizedValue))) {
			return true;
		}

		return false;
	}

	@Override
	public String toString() {
		MoreObjects.ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", operands.size());

		AtomicInteger index = new AtomicInteger();
		operands.stream().limit(AdhocUnsafe.getLimitOrdinalToString()).forEach(filter -> {
			toStringHelper.add("#" + index.getAndIncrement(), filter);
		});
		if (operands.size() > AdhocUnsafe.getLimitOrdinalToString()) {
			int nbMore = operands.size() - AdhocUnsafe.getLimitOrdinalToString();
			toStringHelper.add("#" + AdhocUnsafe.getLimitOrdinalToString(), nbMore + " more entries");
		}

		return toStringHelper.toString();
	}

	/**
	 *
	 * @param allowedValues
	 *            {@link Collection} will be unnested. `null` and `IValueMatchers` are managed specifically.
	 * @return
	 */
	public static IValueMatcher matchIn(Collection<?> allowedValues) {
		Collection<?> unnested = AdhocCollectionHelpers.unnestAsCollection(allowedValues);

		if (unnested.isEmpty()) {
			return MATCH_NONE;
		} else if (unnested.size() == 1) {
			Object singleValue = AdhocCollectionHelpers.getFirst(unnested);
			return EqualsMatcher.matchEq(singleValue);
		}

		Map<Boolean, List<Object>> byValueMatcher =
				unnested.stream().collect(Collectors.partitioningBy(operand -> operand instanceof IValueMatcher));

		List<Object> unnestedNotNull = byValueMatcher.getOrDefault(Boolean.FALSE, List.of());
		List<IValueMatcher> valueMatchers = (List) byValueMatcher.getOrDefault(Boolean.TRUE, List.of());

		List<IValueMatcher> orOperands = new ArrayList<>();

		boolean hasNull = unnestedNotNull.contains(null);
		if (hasNull) {
			orOperands.add(NullMatcher.matchNull());

			unnestedNotNull = unnestedNotNull.stream().filter(Objects::nonNull).toList();
		}
		orOperands.addAll(valueMatchers);
		if (!unnestedNotNull.isEmpty()) {
			IValueMatcher matcher;
			if (unnestedNotNull.size() == 1) {
				matcher = EqualsMatcher.matchEq(unnestedNotNull.getFirst());
			} else {
				matcher = InMatcher.builder().operands(unnestedNotNull).build();
			}
			orOperands.add(matcher);
		}

		// TODO: Skip optimizations between the legs? No as we may consider IValueMatcher covering input values
		return OrMatcher.or(orOperands);
	}

	/**
	 * {@link Collection} will be unnested.
	 *
	 * @return
	 */
	public static IValueMatcher matchIn(Object first, Object second, Object... more) {
		return matchIn(Lists.asList(first, second, more));
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
		String operandsToString = operands.stream()
				.map(String::valueOf)
				.limit(AdhocUnsafe.getLimitOrdinalToString())
				.collect(Collectors.joining(",", "(", ""));

		if (operands.size() > AdhocUnsafe.getLimitOrdinalToString()) {
			int nbMore = operands.size() - AdhocUnsafe.getLimitOrdinalToString();
			operandsToString += ", and " + nbMore + " more entries";
		}

		operandsToString += ")";

		if (negated) {
			return column + "=out=" + operandsToString;
		} else {
			return column + "=in=" + operandsToString;
		}
	}
}

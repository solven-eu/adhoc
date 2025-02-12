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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.solven.adhoc.query.filter.ColumnFilter;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

/**
 * To be used with {@link ColumnFilter}, for AND matchers. True if there is not a single operand.
 *
 * @author Benoit Lacelle
 */
@Builder
@Value
public final class AndMatcher implements IValueMatcher {
	@Singular
	@NonNull
	Set<IValueMatcher> operands;

	public boolean isMatchAll() {
		return operands.isEmpty();
	}

	public static AndMatcherBuilder builder() {
		return new AndMatcherBuilder();
	}

	@Override
	public boolean match(Object value) {
		return operands.stream().allMatch(operand -> operand.match(value));
	}

	public static IValueMatcher and(IValueMatcher... filters) {
		return and(Set.of(filters));
	}

	public static IValueMatcher and(Set<IValueMatcher> filters) {
		if (filters.stream().anyMatch(f -> f instanceof OrMatcher orMatcher && orMatcher.isMatchNone())) {
			return MATCH_NONE;
		}

		// Skipping matchAll is useful on `.edit`
		List<? extends IValueMatcher> notMatchAll = filters.stream()
				.filter(f -> !(f instanceof AndMatcher andMatcher && andMatcher.isMatchAll()))
				.flatMap(operand -> {
					if (operand instanceof AndMatcher operandIsAnd) {
						// AND of ANDs
						return operandIsAnd.getOperands().stream();
					} else {
						return Stream.of(operand);
					}
				})
				.collect(Collectors.toList());

		if (notMatchAll.isEmpty()) {
			return IValueMatcher.MATCH_ALL;
		} else if (notMatchAll.size() == 1) {
			return notMatchAll.getFirst();
		} else {
			return AndMatcher.builder().operands(notMatchAll).build();
		}
	}
}

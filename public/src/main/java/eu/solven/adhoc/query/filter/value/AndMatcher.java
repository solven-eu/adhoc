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
 * To be used with {@link ColumnFilter}, for AND matchers. True if there is not a single operand.
 *
 * Prefer `.and(...)` to optimize the matcher, except if you need an unoptimized `AndMatcher`.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
@Jacksonized
public final class AndMatcher implements IValueMatcher, IHasOperands<IValueMatcher> {
	@NonNull
	@Singular
	ImmutableSet<IValueMatcher> operands;

	@JsonIgnore
	public boolean isMatchAll() {
		return operands.isEmpty();
	}

	@Override
	public String toString() {
		if (isMatchAll()) {
			return "matchAll";
		}

		int size = operands.size();
		if (size <= AdhocUnsafe.limitOrdinalToString) {
			return operands.stream().map(Object::toString).collect(Collectors.joining("&"));
		} else {
			MoreObjects.ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", size);

			AtomicInteger index = new AtomicInteger();
			operands.stream().limit(AdhocUnsafe.limitOrdinalToString).forEach(filter -> {
				toStringHelper.add("#" + index.getAndIncrement(), filter);
			});

			return toStringHelper.toString();
		}
	}

	@Override
	public boolean match(Object value) {
		return operands.stream().allMatch(operand -> operand.match(value));
	}

	// `first, second, more` syntax to push providing at least 2 arguments
	public static IValueMatcher and(IValueMatcher first, IValueMatcher second, IValueMatcher... more) {
		return and(Lists.asList(first, second, more));
	}

	public static IValueMatcher and(Collection<? extends IValueMatcher> filters) {
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

		notMatchAll = simplifiedOwned(notMatchAll);

		if (notMatchAll.isEmpty()) {
			return IValueMatcher.MATCH_ALL;
		} else if (notMatchAll.size() == 1) {
			return notMatchAll.getFirst();
		} else {
			return AndMatcher.builder().operands(notMatchAll).build();
		}
	}

	// TODO Refactor with OR
	private static List<? extends IValueMatcher> simplifiedOwned(List<? extends IValueMatcher> matchers) {
		List<IValueMatcher> simplified = new ArrayList<>();

		for (IValueMatcher matcher : matchers) {
			if (simplified.stream().anyMatch(alreadyIn -> isStricter(alreadyIn, matcher))) {
				// matcher can be skipped
			} else {
				// Remove those owned
				simplified.removeIf(alreadyIn -> isStricter(matcher, alreadyIn));
				// Then add this matcher
				simplified.add(matcher);
			}
		}

		return simplified;
	}

	/**
	 * 
	 * @param owner
	 * @param owned
	 * @return true if `owner AND owned` is equivalent to `owner`
	 */
	static boolean isStricter(IValueMatcher owner, IValueMatcher owned) {
		if (owner.equals(owned)) {
			return true;
		} else if (owner instanceof ComparingMatcher ownerComparing) {
			if (owned instanceof ComparingMatcher ownedComparing) {
				if (ownerComparing.isGreaterThan() != ownedComparing.isGreaterThan()) {
					// `<=` can not own `>=`
					return false;
				} else if (ownerComparing.isMatchIfEqual() != ownedComparing.isMatchIfEqual()) {
					return false;
				} else if (ownerComparing.isMatchIfNull() != ownedComparing.isMatchIfNull()) {
					// `null` needs to be managed the same way for ownership
					return false;
				}

				Object ownerOperand = ownerComparing.getOperand();
				Object ownedOperand = ownedComparing.getOperand();
				if (ownerOperand.getClass() != ownedOperand.getClass()) {
					// Different class may lead to Comparing issues
					return false;
				} else {
					int ownerMinusOwned = ((Comparable) ownerOperand).compareTo(ownedOperand);

					if (ownerComparing.isGreaterThan() && ownerMinusOwned >= 0) {
						// `a >= X && a >= Y && X >= Y` ==> `a >= X`
						return true;
					}
				}
			}
		}

		return false;
	}
}

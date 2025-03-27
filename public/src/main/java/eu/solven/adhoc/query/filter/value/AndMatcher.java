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
import java.util.Optional;
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
		return and(filters, true);
	}

	public static IValueMatcher and(Collection<? extends IValueMatcher> filters, boolean doSimplify) {
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

		if (doSimplify) {
			notMatchAll = simplifiedOwned(notMatchAll);
		}

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
			List<IValueMatcher> withMatcher = new ArrayList<>(matchers.size() + 1);

			boolean matcherIsIncluded = false;
			for (IValueMatcher alreadyIn : simplified) {
				Optional<IValueMatcher> optMerged = merge(alreadyIn, matcher);

				if (optMerged.isPresent()) {
					matcherIsIncluded = true;
					withMatcher.add(optMerged.get());
				} else {
					matcherIsIncluded = false;
					withMatcher.add(alreadyIn);
				}
			}

			if (!matcherIsIncluded) {
				// matcher can be skipped. It is already included in previous step
				withMatcher.add(matcher);
			}

			simplified = withMatcher;
		}

		return simplified;
	}

	/**
	 * 
	 * @param owner
	 * @param owned
	 * @return true an Optional {@link IValueMatcher} equivalent to `owner AND owned`. If empty, it is equivalent to
	 *         returning `owner AND owned`.
	 */
	static Optional<IValueMatcher> merge(IValueMatcher owner, IValueMatcher owned) {
		Optional<IValueMatcher> mergedNotSwapped = rawMerge(owner, owned);
		if (mergedNotSwapped.isPresent()) {
			return mergedNotSwapped;
		}

		// We try swapping as the merge output is the best merge whatever the order of arguments
		// `rawMerge` does not swap its arguments for simpler implementation
		Optional<IValueMatcher> mergedSwapped = rawMerge(owned, owner);
		if (mergedSwapped.isPresent()) {
			return mergedSwapped;
		}

		return Optional.empty();
	}

	/**
	 * This will typically be called with `arg1` and `arg2`: the implementation does not need to swap the arguments.
	 * 
	 * @param owner
	 * @param owned
	 * @return
	 */
	private static Optional<IValueMatcher> rawMerge(IValueMatcher owner, IValueMatcher owned) {
		if (owner.equals(owned)) {
			return Optional.of(owner);
		} else if (MATCH_NONE.equals(owner) || MATCH_NONE.equals(owned)) {
			return Optional.of(MATCH_NONE);
		} else if (owned instanceof ComparingMatcher ownedComparing) {
			if (owner instanceof ComparingMatcher ownerComparing) {
				if (ownerComparing.isMatchIfEqual() != ownedComparing.isMatchIfEqual()) {
					return Optional.empty();
				} else if (ownerComparing.isMatchIfNull() != ownedComparing.isMatchIfNull()) {
					// `null` needs to be managed the same way for ownership
					return Optional.empty();
				}

				Object ownerOperand = ownerComparing.getOperand();
				Object ownedOperand = ownedComparing.getOperand();
				if (ownerOperand.getClass() != ownedOperand.getClass()) {
					// Different class may lead to Comparing issues
					return Optional.empty();
				} else {
					int ownerMinusOwned = ((Comparable) ownerOperand).compareTo(ownedOperand);

					if (ownerComparing.isGreaterThan() != ownedComparing.isGreaterThan()) {
						// Either this is a between, or this is a matchNone
						if (ownerComparing.isGreaterThan() && ownerMinusOwned > 0) {
							// `a >= X && a <= Y && X >= Y` ==> `matchNone`
							return Optional.of(IValueMatcher.MATCH_NONE);
						} else if (!ownerComparing.isGreaterThan() && ownerMinusOwned < 0) {
							// `a <= X && a >= Y && X <= Y` ==> `matchNone`
							return Optional.of(IValueMatcher.MATCH_NONE);
						} else {
							// `a >= X && a <= Y && X <= Y` ==> `matchNone`
							return Optional.empty();
						}
					} else {
						// Necessarily owned

						if (ownerComparing.isGreaterThan()) {
							if (ownerMinusOwned >= 0) {
								// `a >= X && a >= Y && X >= Y` ==> `a >= X`
								// `a > X && a > Y && X >= Y` ==> `a > X`
								return Optional.of(owner);
							} else {
								// `a >= X && a >= Y && X <= Y` ==> `a >= Y`
								// `a > X && a > Y && X <= Y` ==> `a > Y`
								return Optional.of(owned);
							}
						} else if (!ownerComparing.isGreaterThan()) {
							// `a <= X && a <= Y && X <= Y` ==> `a <= X`
							// `a < X && a < Y && X <= Y` ==> `a < X`
							if (ownerMinusOwned <= 0) {
								return Optional.of(owner);
							} else {
								return Optional.of(owned);
							}
						}
					}

				}
			} else if (owner instanceof EqualsMatcher ownerEquals) {
				Object ownedOperand = ownedComparing.getOperand();
				Object ownerOperand = ownerEquals.getOperand();
				if (ownerOperand.getClass() != ownedOperand.getClass()) {
					// Different class may lead to Comparing issues
					return Optional.empty();
				} else {
					int ownerMinusOwned = ((Comparable) ownerOperand).compareTo(ownedOperand);

					if (ownerMinusOwned == 0) {
						if (ownedComparing.isMatchIfEqual()) {
							// `a == X && a >= Y && X == Y` ==> `a == X`
							return Optional.of(ownerEquals);
						} else {
							return Optional.of(IValueMatcher.MATCH_NONE);
						}
					} else if (ownedComparing.isGreaterThan()) {
						if (ownerMinusOwned > 0) {
							// `a == X && a >= Y && X > Y` ==> `a >= X`
							return Optional.of(ownerEquals);
						} else {
							// `a == X && a >= Y && X < Y` ==> `matchNone`
							return Optional.of(IValueMatcher.MATCH_NONE);
						}
					} else if (!ownedComparing.isGreaterThan() && ownerMinusOwned < 0) {
						if (ownerMinusOwned > 0) {
							// `a == X && a <= Y && X < Y` ==> `a >= X`
							return Optional.of(ownerEquals);
						} else {
							// `a == X && a <= Y && X > Y` ==> `matchNone`
							return Optional.of(IValueMatcher.MATCH_NONE);
						}
					}
				}
			} else if (owner instanceof NullMatcher ownedNull) {
				if (ownedComparing.isMatchIfNull()) {
					// Can only be `null`
					return Optional.of(ownedNull);
				} else {
					// Can not be both `null` and `!null`
					return Optional.of(IValueMatcher.MATCH_NONE);
				}
			}
		}

		return Optional.empty();
	}
}

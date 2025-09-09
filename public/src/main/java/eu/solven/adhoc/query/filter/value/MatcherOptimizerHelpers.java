/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
import java.util.List;
import java.util.Optional;
import java.util.Set;

import lombok.experimental.UtilityClass;

/**
 * Holds optimizations related to {@link IValueMatcher}.
 *
 * @author Benoit Lacelle
 */
@SuppressWarnings("PMD.GodClass")
@UtilityClass
public class MatcherOptimizerHelpers {

	/**
	 *
	 * @param matchers
	 *            a {@link List} of {@link IValueMatcher}
	 * @return a shorter {@link List} of {@link IValueMatcher} considering they would be AND-ed together.
	 */
	public static List<? extends IValueMatcher> simplifiedOwned(List<? extends IValueMatcher> matchers) {
		List<IValueMatcher> simplified = new ArrayList<>();

		for (IValueMatcher matcher : matchers) {
			List<IValueMatcher> withMatcher = new ArrayList<>(matchers.size() + 1);

			boolean matcherIsIncluded = false;
			for (IValueMatcher alreadyIn : simplified) {
				Optional<IValueMatcher> optMerged = mergeMatchers(alreadyIn, matcher);

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
	 * @return an Optional {@link IValueMatcher} equivalent to `owner AND owned`. If empty, it is equivalent to
	 *         returning `owner AND owned`.
	 */
	@SuppressWarnings("PMD.CognitiveComplexity")
	static Optional<IValueMatcher> mergeMatchers(IValueMatcher owner, IValueMatcher owned) {
		if (owner.equals(owned)) {
			return Optional.of(owner);
		} else if (IValueMatcher.MATCH_NONE.equals(owner) || IValueMatcher.MATCH_NONE.equals(owned)) {
			return Optional.of(IValueMatcher.MATCH_NONE);
		} else {
			// Equals
			if (owned instanceof EqualsMatcher ownedEquals) {
				return Optional.of(mergeGivenEquals(ownedEquals, owner));
			}
			if (owner instanceof EqualsMatcher ownerEquals) {
				return Optional.of(mergeGivenEquals(ownerEquals, owned));
			}

			// In
			if (owned instanceof InMatcher ownedIn) {
				return Optional.of(mergeGivenIn(ownedIn, owner));
			}
			if (owner instanceof InMatcher ownerIn) {
				return Optional.of(mergeGivenIn(ownerIn, owned));
			}

			// Null
			if (owned instanceof NullMatcher ownedNull) {
				return Optional.of(mergeGivenNull(ownedNull, owner));
			}
			if (owner instanceof NullMatcher ownerNull) {
				return Optional.of(mergeGivenNull(ownerNull, owned));
			}

			// NotMatcher
			if (owned instanceof NotMatcher ownedNot) {
				Optional<IValueMatcher> optSimplifiedWithNot = mergeGivenNot(ownedNot, owner);
				if (optSimplifiedWithNot.isPresent()) {
					return optSimplifiedWithNot;
				}
			}
			if (owner instanceof NotMatcher ownerNot) {
				Optional<IValueMatcher> optSimplifiedWithNot = mergeGivenNot(ownerNot, owned);
				if (optSimplifiedWithNot.isPresent()) {
					return optSimplifiedWithNot;
				}
			}

			// Comparing
			if (owned instanceof ComparingMatcher ownedComparing && owner instanceof ComparingMatcher ownerComparing) {
				Optional<IValueMatcher> optSimplified = mergeComparing(ownedComparing, ownerComparing);
				if (optSimplified.isPresent()) {
					return optSimplified;
				}
			}
		}

		return Optional.empty();
	}

	/**
	 * Typically turns `>=10 AND >13` into `>13`.
	 * 
	 * @param owned
	 * @param owner
	 * @return an {@link Optional} {@link IValueMatcher} which is equivalent to the 2 input {@link ComparingMatcher}
	 *         being `AND` together. If empty, it means the input should be combined with an `AndMatcher`.
	 */
	private static Optional<IValueMatcher> mergeComparing(ComparingMatcher owned, ComparingMatcher owner) {
		if (owner.isMatchIfEqual() != owned.isMatchIfEqual()) {
			return Optional.empty();
		} else if (owner.isMatchIfNull() != owned.isMatchIfNull()) {
			// `null` needs to be managed the same way for ownership
			return Optional.empty();
		}

		Object ownerOperand = owner.getOperand();
		Object ownedOperand = owned.getOperand();
		if (ownerOperand.getClass() != ownedOperand.getClass()) {
			// Different class may lead to Comparing issues
			return Optional.empty();
		} else {
			int ownerMinusOwned = ((Comparable) ownerOperand).compareTo(ownedOperand);

			if (owner.isGreaterThan() != owned.isGreaterThan()) {
				// Either this is a between, or this is a matchNone
				if (owner.isGreaterThan() && ownerMinusOwned > 0) {
					// `a >= X && a <= Y && X >= Y` ==> `matchNone`
					return Optional.of(IValueMatcher.MATCH_NONE);
				} else if (!owner.isGreaterThan() && ownerMinusOwned < 0) {
					// `a <= X && a >= Y && X <= Y` ==> `matchNone`
					return Optional.of(IValueMatcher.MATCH_NONE);
				} else {
					// `a >= X && a <= Y && X <= Y` ==> `matchNone`
					return Optional.empty();
				}
			} else {
				// Necessarily owned

				if (owner.isGreaterThan()) {
					if (ownerMinusOwned >= 0) {
						// `a >= X && a >= Y && X >= Y` ==> `a >= X`
						// `a > X && a > Y && X >= Y` ==> `a > X`
						return Optional.of(owner);
					} else {
						// `a >= X && a >= Y && X <= Y` ==> `a >= Y`
						// `a > X && a > Y && X <= Y` ==> `a > Y`
						return Optional.of(owned);
					}
				} else if (!owner.isGreaterThan()) {
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

		return Optional.empty();
	}

	/**
	 *
	 * @param equalsMatcher
	 * @param other
	 * @return EqualsMatcher or matchNone
	 */
	private static IValueMatcher mergeGivenEquals(EqualsMatcher equalsMatcher, IValueMatcher other) {
		Object operand = equalsMatcher.getOperand();

		if (other.match(operand)) {
			return equalsMatcher;
		} else {
			return IValueMatcher.MATCH_NONE;
		}
	}

	/**
	 *
	 * @param nullMatcher
	 * @param other
	 * @return {@link NullMatcher} or matchNone
	 */
	private static IValueMatcher mergeGivenNull(NullMatcher nullMatcher, IValueMatcher other) {
		if (other.match(null)) {
			return nullMatcher;
		} else {
			return IValueMatcher.MATCH_NONE;
		}
	}

	/**
	 *
	 * @param notMatcher
	 * @param other
	 * @return an Optional {@link IValueMatcher} equivalent to `owner AND owned`. If empty, it is equivalent to
	 *         returning `owner AND owned`.
	 */
	private static Optional<IValueMatcher> mergeGivenNot(NotMatcher notMatcher, IValueMatcher other) {
		IValueMatcher negated = notMatcher.getNegated();

		if (negated instanceof EqualsMatcher negatedEquals) {
			if (!other.match(negatedEquals.getOperand())) {
				// `other` does not accept the negated element in the `not`
				// So the `not` is embedded in `other`
				return Optional.of(other);
			}

			if (other instanceof NotMatcher otherNot) {
				if (otherNot.getNegated() instanceof EqualsMatcher otherNotNegated) {
					// `!= x && != y`
					return Optional.of(
							NotMatcher.not(InMatcher.isIn(negatedEquals.getOperand(), otherNotNegated.getOperand())));
				} else if (otherNot.getNegated() instanceof InMatcher otherNotNegated) {
					// `!= x && =out= y`
					return Optional.of(
							NotMatcher.not(InMatcher.isIn(negatedEquals.getOperand(), otherNotNegated.getOperands())));
				}
			}

		} else if (negated instanceof InMatcher negatedIn) {
			Set<?> disallowedElements = negatedIn.getOperands();

			if (other instanceof NotMatcher otherNot) {
				if (otherNot.getNegated() instanceof EqualsMatcher otherNotNegated) {
					// `=out= x && != y`
					return Optional
							.of(NotMatcher.not(InMatcher.isIn(disallowedElements, otherNotNegated.getOperand())));
				} else if (otherNot.getNegated() instanceof InMatcher otherNotNegated) {
					// `!= x && =out= y`
					return Optional
							.of(NotMatcher.not(InMatcher.isIn(disallowedElements, otherNotNegated.getOperands())));
				}
			}

			List<?> disallowedButAllowedByOther = disallowedElements.stream().filter(other::match).toList();

			if (disallowedButAllowedByOther.isEmpty()) {
				// The whole not is already rejected by `other`
				return Optional.of(other);
			}

			if (disallowedButAllowedByOther.size() < disallowedElements.size()) {
				AndMatcher simplerWithNotAndIn = AndMatcher.builder()
						// Reject individually the elements not already rejected by other
						.operand(NotMatcher.not(InMatcher.isIn(disallowedButAllowedByOther), false))
						.operand(other)
						.build();
				return Optional.of(simplerWithNotAndIn);
			}

		}

		return Optional.empty();
	}

	/**
	 *
	 * @param inMatcher
	 * @param other
	 * @return InMatcher possibly simplified in Equals or none
	 */
	private static IValueMatcher mergeGivenIn(InMatcher inMatcher, IValueMatcher other) {
		Set<?> inOperands = inMatcher.getOperands();

		List<?> allowedInBoth = inOperands.stream().filter(other::match).toList();

		return InMatcher.isIn(allowedInBoth);
	}
}

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

import java.util.Set;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * `not` or `!` boolean operator. It negates the underlying {@link IValueMatcher}.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
@Jacksonized
public class NotMatcher implements IValueMatcher, IColumnToString {

	@NonNull
	final IValueMatcher negated;

	@Override
	public boolean match(Object value) {
		return !negated.match(value);
	}

	public static IValueMatcher not(IValueMatcher negated) {
		return not(negated, true);
	}

	/**
	 * 
	 * @param negated
	 * @param doSimplify
	 *            may be false when we when optimizations are already applied, to break simplification cycles
	 * @return
	 */
	public static IValueMatcher not(IValueMatcher negated, boolean doSimplify) {
		if (negated instanceof ComparingMatcher comparing) {
			return ComparingMatcher.builder()
					.greaterThan(!comparing.isGreaterThan())
					.matchIfEqual(!comparing.isMatchIfEqual())
					.matchIfNull(!comparing.isMatchIfNull())
					.operand(comparing.getOperand())
					.build();
		} else if (negated instanceof NotMatcher not) {
			return not.getNegated();
		} else if (negated instanceof AndMatcher and) {
			Set<IValueMatcher> operands = and.getOperands();

			// `!AND` is same as `OR(!)`
			return OrMatcher.or(operands.stream().map(NotMatcher::not).toList(), doSimplify);
		} else if (negated instanceof OrMatcher or) {
			Set<IValueMatcher> operands = or.getOperands();

			// `!OR` is same as `AND(!)`
			return AndMatcher.and(operands.stream().map(NotMatcher::not).toList(), doSimplify);
		}

		return NotMatcher.builder().negated(negated).build();
	}

	@Override
	public String toString(String column, boolean isNegated) {
		if (negated instanceof IColumnToString customToString) {
			return customToString.toString(column, !isNegated);
		} else {
			// Similar to eu.solven.adhoc.query.filter.ColumnFilter.toString
			return "%s does NOT match `%s`".formatted(column, negated);
		}
	}
}
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

import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;

import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.query.filter.ColumnFilter;

/**
 * To be used with {@link ColumnFilter}, for equality-based matchers.
 * 
 * Equality in Adhoc is not a pure `hashcode/equals` equality. Typically, `(int) 3` is equal to `(long) 3` and `(float)
 * 3` is equal to `(double) 3`.
 * 
 * @author Benoit Lacelle
 *
 */
// BEWARE This is not a strict `equals`, as it has special rules around ints and longs
// We may introduce a StrictEqualsMatcher
// BEWARE Should we introduce a way to match primitive value without boxing?
public abstract class EqualsMatcher implements IValueMatcher, IHasWrapped, IColumnToString {

	public abstract Object getOperand();

	@Override
	public String toString() {
		return "==%s".formatted(getWrapped());
	}

	@Override
	public String toString(String column, boolean negated) {
		// https://github.com/jirutka/rsql-parser?tab=readme-ov-file#grammar-and-semantic
		if (negated) {
			return "%s!=%s".formatted(column, getWrapped());
		} else {
			return "%s==%s".formatted(column, getWrapped());
		}
	}

	@JsonCreator
	public static IValueMatcher createWrapped(Object o) {
		if (o instanceof Map<?, ?> map) {
			// Workaround some Jackson issue/limitations
			return isEqualTo(map.get("operand"));
		} else {
			return isEqualTo(o);
		}
	}

	/**
	 * 
	 * @param operand
	 *            typically a value for which `.equals` is relevant. `null` and `IValueMatcher` are special cases.
	 * @return
	 */
	@SuppressWarnings("PMD.LinguisticNaming")
	public static IValueMatcher isEqualTo(Object operand) {
		if (operand == null) {
			return NullMatcher.matchNull();
		} else if (operand instanceof IValueMatcher valueMatcher) {
			// Typically used by CubeWrapperTypeTranscoder
			return valueMatcher;
		} else if (AdhocPrimitiveHelpers.isLongLike(operand)) {
			return EqualsLongMatcher.builder().operand(AdhocPrimitiveHelpers.asLong(operand)).build();
		} else if (AdhocPrimitiveHelpers.isDoubleLike(operand)) {
			return EqualsDoubleMatcher.builder().operand(AdhocPrimitiveHelpers.asDouble(operand)).build();
		} else {
			return new EqualsObjectMatcher(operand);
		}
	}

	/**
	 *
	 * @param valueMatcher
	 *            some {@link IValueMatcher} expected to be an {@link EqualsMatcher}
	 * @param clazz
	 *            the expected type of the operand
	 * @return an {@link Optional} of the operand, if {@link IValueMatcher} is an {@link EqualsMatcher} and its operand
	 *         is an instance of clazz.
	 * @param <T>
	 */
	public static <T> Optional<T> extractOperand(IValueMatcher valueMatcher, Class<T> clazz) {
		if (!(valueMatcher instanceof EqualsMatcher equalsMatcher)) {
			return Optional.empty();
		} else {
			Object operand = equalsMatcher.getOperand();

			if (clazz.isInstance(operand)) {
				return Optional.of(clazz.cast(operand));
			} else {
				return Optional.empty();
			}
		}
	}

	/**
	 *
	 * @param valueMatcher
	 *            some {@link IValueMatcher} expected to be an {@link EqualsMatcher}
	 * @return an {@link Optional} of the operand, if {@link IValueMatcher} is an {@link EqualsMatcher}
	 */
	public static Optional<?> extractOperand(IValueMatcher valueMatcher) {
		return extractOperand(valueMatcher, Object.class);
	}
}

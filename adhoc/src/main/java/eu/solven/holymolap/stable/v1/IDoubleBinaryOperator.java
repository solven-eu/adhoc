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
package eu.solven.holymolap.stable.v1;

import java.util.function.DoubleBinaryOperator;

/**
 * Represents an operation upon two {@code double}-valued operands, producing a {@code double}-valued result.
 */
public interface IDoubleBinaryOperator extends DoubleBinaryOperator, IBinaryOperator {
	/**
	 * 
	 * @return the value of an empty aggregate, or a row with no value for the aggregated column.
	 */
	double neutralAsDouble();

	/**
	 * Applies this operator to the given operands.
	 *
	 * @param left
	 *            the first operand
	 * @param right
	 *            the second operand
	 * @return the operator result
	 */
	@Override
	double applyAsDouble(double left, double right);

	@Override
	@Deprecated
	default Object neutral() {
		return neutralAsDouble();
	}

	@Override
	@Deprecated
	default Object apply(Object left, Object right) {
		if (left == null) {
			return right;
		} else if (right == null) {
			return left;
		}
		return applyAsDouble((((Number) left).doubleValue()), (((Number) right).doubleValue()));
	}
}
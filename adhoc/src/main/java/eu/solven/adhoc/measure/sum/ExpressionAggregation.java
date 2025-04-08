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
package eu.solven.adhoc.measure.sum;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.model.Aggregator;

/**
 * Used when the aggregation is an expression for {@link eu.solven.adhoc.table.ITableWrapper}.
 * 
 * The actual expression is provided through the {@link Aggregator} column.
 *
 * @author Benoit Lacelle
 */
public class ExpressionAggregation implements IAggregation {
	public static final String KEY = "expression";

	@Override
	public Object aggregate(Object left, Object right) {
		if (left == null) {
			return right;
		} else if (right == null) {
			return left;
		} else {
			throw new UnsupportedOperationException("Can not be evaluated. left=%s right=%s".formatted(left, right));
		}
	}

	public static boolean isExpression(String aggregationKey) {
		return KEY.equals(aggregationKey) || ExpressionAggregation.class.getName().equals(aggregationKey);
	}
}

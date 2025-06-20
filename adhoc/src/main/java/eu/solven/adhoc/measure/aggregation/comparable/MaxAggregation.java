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
package eu.solven.adhoc.measure.aggregation.comparable;

import java.util.Comparator;

import eu.solven.adhoc.map.ComparableElseClassComparatorV2;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.IDoubleAggregation;
import eu.solven.adhoc.measure.aggregation.ILongAggregation;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import lombok.experimental.SuperBuilder;

/**
 * Keep the highest value amongst encountered values
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
public class MaxAggregation implements IAggregation, IDoubleAggregation, ILongAggregation {

	public static final String KEY = "MAX";

	final Comparator<Object> comparator = new ComparableElseClassComparatorV2();

	@Override
	public Object aggregate(Object l, Object r) {
		if (l == null) {
			return r;
		} else if (r == null) {
			return l;
		} else {
			if (l instanceof Number lAsNumber && r instanceof Number rAsNumber) {
				if (AdhocPrimitiveHelpers.isLongLike(l) && AdhocPrimitiveHelpers.isLongLike(r)) {
					long leftAsLong = lAsNumber.longValue();
					long rightAsLong = rAsNumber.longValue();

					return aggregateLongs(leftAsLong, rightAsLong);
				} else {
					double leftAsDouble = lAsNumber.doubleValue();
					double rightAsDouble = rAsNumber.doubleValue();

					return aggregateDoubles(leftAsDouble, rightAsDouble);
				}
			} else {
				int compared = compare(l, r);

				if (compared >= 0) {
					// If equals, we keep l
					return l;
				} else {
					return r;
				}
			}
		}
	}

	protected int compare(Object l, Object r) {
		return comparator.compare(l, r);
	}

	@Override
	public double aggregateDoubles(double left, double right) {
		return Double.max(left, right);
	}

	@Override
	public long aggregateLongs(long left, long right) {
		return Long.max(left, right);
	}

	@Override
	public long neutralLong() {
		return Long.MIN_VALUE;
	}
}

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
package eu.solven.adhoc.measure.sum;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.ILongAggregation;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.pepper.core.PepperLogHelper;

/**
 * Count the number of received entries. Typically used for `COUNT(*)` row count.
 */
public class CountAggregation implements IAggregation, ILongAggregation {

	public static final String KEY = "COUNT";

	@Override
	public Object aggregate(Object l, Object r) {
		if (l == null) {
			return aggregateOne(r);
		} else if (r == null) {
			return aggregateOne(l);
		} else {
			if (!AdhocPrimitiveHelpers.isLongLike(l)) {
				throw new IllegalArgumentException(
						"Can not aggregate in count: %s".formatted(PepperLogHelper.getObjectAndClass(l)));
			} else if (!AdhocPrimitiveHelpers.isLongLike(r)) {
				throw new IllegalArgumentException(
						"Can not aggregate in count: %s".formatted(PepperLogHelper.getObjectAndClass(r)));
			}

			return AdhocPrimitiveHelpers.asLong(l) + AdhocPrimitiveHelpers.asLong(r);
		}
	}

	protected Object aggregateOne(Object one) {
		if (AdhocPrimitiveHelpers.isLongLike(one)) {
			return AdhocPrimitiveHelpers.asLong(one);
		} else {
			throw new IllegalArgumentException(
					"Can not aggregate in count: %s".formatted(PepperLogHelper.getObjectAndClass(one)));
		}
	}

	@Override
	public long aggregateLongs(long left, long right) {
		return left + right;
	}

	@Override
	public long neutralLong() {
		return 0;
	}

	// @Override
	// public CountHolder wrap(Object v) {
	// return CountHolder.builder().count(((Number) v).longValue()).build();
	// }
}

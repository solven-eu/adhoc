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
package eu.solven.adhoc.aggregations.sum;

import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.IDoubleAggregation;
import eu.solven.adhoc.aggregations.ILongAggregation;

/**
 * Like {@link SumAggregator}, but ignores {@link Double#NaN}
 * 
 * @author Benoit Lacelle
 *
 */
public class SumNotNaNAggregator implements IAggregation, IDoubleAggregation, ILongAggregation {

	public static final String KEY = "SUM";

	@Override
	public Object aggregate(Object l, Object r) {
		if (l == null) {
			return r;
		} else if (r == null) {
			return l;
		} else if (isLongLike(l) && isLongLike(r)) {
			return aggregateLongs(((Number) l).longValue(), ((Number) r).longValue());
		} else {
			return aggregateDoubles(((Number) l).doubleValue(), ((Number) r).doubleValue());
		}
	}

	@Override
	public double aggregateDoubles(double l, double r) {
		if (Double.isNaN(l)) {
			if (Double.isNaN(r)) {
				return 0D;
			} else {
				return r;
			}
		} else if (Double.isNaN(r)) {
			return l;
		} else {
			return l + r;
		}
	}

	@Override
	public long aggregateLongs(long l, long r) {
		return l + r;
	}

	public static boolean isLongLike(Object o) {
		return Integer.class.isInstance(o) || Long.class.isInstance(o);
	}
}

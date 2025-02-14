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
import eu.solven.adhoc.measure.aggregation.IDoubleAggregation;
import eu.solven.adhoc.measure.aggregation.ILongAggregation;
import lombok.extern.slf4j.Slf4j;

/**
 * A `PRODUCT` {@link IAggregation}. It will aggregate as longs, doubles or Strings depending on the inputs.
 */
// https://learn.microsoft.com/en-us/dax/product-function-dax
@Slf4j
public class ProductAggregation implements IAggregation, IDoubleAggregation, ILongAggregation {

	public static final String KEY = "PRODUCT";

	@Override
	public Object aggregate(Object l, Object r) {
		if (l == null) {
			return r;
		} else if (r == null) {
			return l;
		} else if (SumAggregation.isLongLike(l) && SumAggregation.isLongLike(r)) {
			return aggregateLongs(asLong(l), asLong(r));
		} else if (SumAggregation.isDoubleLike(l) && SumAggregation.isDoubleLike(r)) {
			return aggregateDoubles(asDouble(l), asDouble(r));
		} else {
			throw new IllegalArgumentException("Can not %s on (`%s`, `%s`)".formatted(KEY, l, r));
		}
	}

	@Override
	public double aggregateDoubles(double left, double right) {
		return left * right;
	}

	@Override
	public long aggregateLongs(long left, long right) {
		return left * right;
	}

	public static long asLong(Object o) {
		return ((Number) o).longValue();
	}

	public static double asDouble(Object o) {
		return ((Number) o).doubleValue();
	}
}

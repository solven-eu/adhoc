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
package eu.solven.adhoc.aggregations.collection;

import eu.solven.adhoc.aggregations.IAggregation;

public class UnionAggregator implements IAggregation {

	public static final String KEY = "UNION";

	private final MapAggregator<Object, Object> mapAggregator = new MapAggregator<>();
	private final UnionSetAggregator<Object> setAggregator = new UnionSetAggregator<>();

	@Override
	public Object aggregate(Object l, Object r) {
		if (mapAggregator.acceptAggregate(l) && mapAggregator.acceptAggregate(r)) {
			return mapAggregator.aggregate(l, r);
		} else if (setAggregator.acceptAggregate(l) && setAggregator.acceptAggregate(r)) {
			return setAggregator.aggregate(l, r);
		} else {
			throw new IllegalArgumentException("No strategy in %s to merge %s and %s".formatted(KEY, l, r));
		}
	}

	// @Override
	// public double aggregateDoubles(double l, double r) {
	// if (setAggregator.acceptAggregate(l) && setAggregator.acceptAggregate(r)) {
	// return setAggregator.aggregateDoubles(l, r);
	// } else {
	// throw new IllegalArgumentException("No strategy in %s to merge %s and %s".formatted(KEY, l, r));
	// }
	// }

	// @Override
	// public long aggregateLongs(long l, long r) {
	// if (setAggregator.acceptAggregate(l) && setAggregator.acceptAggregate(r)) {
	// return setAggregator.aggregateLongs(l, r);
	// } else {
	// throw new IllegalArgumentException("No strategy in %s to merge %s and %s".formatted(KEY, l, r));
	// }
	// }
}

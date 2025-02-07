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
package eu.solven.adhoc.measure.aggregation.collection;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.measure.aggregation.IAggregation;

public class UnionSetAggregator<K> implements IAggregation {

	public static final String KEY = "UNION_SET";

	public boolean acceptAggregate(Object o) {
		return o instanceof Set || o == null;
	}

	@Override
	public Set<K> aggregate(Object l, Object r) {
		Set<?> lAsSet = (Set<?>) l;
		Set<?> rAsSet = (Set<?>) r;

		return aggregateSets(lAsSet, rAsSet);
	}

	protected Set<K> aggregateSets(Set<?> l, Set<?> r) {
		if (l == null || l.isEmpty()) {
			return (Set<K>) r;
		} else if (r == null || r.isEmpty()) {
			return (Set<K>) l;
		} else {
			return (Set<K>) ImmutableSet.builder().addAll(l).addAll(r).build();
		}
	}

	// @Override
	// public double aggregateDoubles(double left, double right) {
	// throw new UnsupportedOperationException("Can not %s on doubles".formatted(KEY));
	// }

	// @Override
	// public long aggregateLongs(long left, long right) {
	// throw new UnsupportedOperationException("Can not %s on longs".formatted(KEY));
	// }

	/**
	 * This is useful to be used in {@link Map#}.merge
	 * 
	 * @param <T>
	 * @param left
	 * @param right
	 * @return
	 */
	public static <T> Set<T> unionSet(Set<T> left, Set<T> right) {
		return new UnionSetAggregator<T>().aggregateSets(left, right);
	}
}

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

import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.aggregations.IAggregation;

public class UnionListAggregator<K> implements IAggregation {

	public static final String KEY = "UNION_LIST";

	public boolean acceptAggregate(Object o) {
		return o instanceof List || o == null;
	}

	@Override
	public List<K> aggregate(Object l, Object r) {
		List<K> lAsList = (List<K>) l;
		List<K> rAsList = (List<K>) r;

		return aggregateLists(lAsList, rAsList);
	}

	protected List<K> aggregateLists(List<K> l, List<K> r) {
		if (l == null || l.isEmpty()) {
			return r;
		} else if (r == null || r.isEmpty()) {
			return l;
		} else {
			return (List<K>) ImmutableList.builder().addAll(l).addAll(r).build();
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
	public static <T> List<T> unionList(List<T> left, List<T> right) {
		return new UnionListAggregator<T>().aggregateLists(left, right);
	}
}

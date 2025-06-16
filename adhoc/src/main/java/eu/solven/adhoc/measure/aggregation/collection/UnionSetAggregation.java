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

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.measure.aggregation.IAggregation;

/**
 * {@link IAggregation} which essentially merge {@link Set}.
 * 
 * @author Benoit Lacelle
 */
public class UnionSetAggregation extends AUnionCollectionAggregation {

	public static final String KEY = "UNION_SET";

	// https://duckdb.org/docs/stable/sql/query_syntax/unnest.html
	public static final String K_UNNEST = "unnest";

	public UnionSetAggregation() {
		super();
	}

	public UnionSetAggregation(Map<String, ?> options) {
		super(options);
	}

	public boolean acceptAggregate(Object o) {
		return o instanceof Set || o == null;
	}

	@Override
	@SuppressWarnings("PMD.ReturnEmptyCollectionRatherThanNull")
	protected Set<?> onEmpty() {
		return null;
	}

	@Override
	public Set<?> aggregate(Object l, Object r) {
		Set<?> lAsSet = wrapAsSet(l);
		Set<?> rAsSet = wrapAsSet(r);

		Set<?> aggregated = aggregateSets(lAsSet, rAsSet);

		if (aggregated.isEmpty()) {
			return onEmpty();
		}
		return aggregated;
	}

	protected Set<?> wrapAsSet(Object l) {
		Set<?> asSet;
		if (l == null) {
			asSet = onEmpty();
		} else if (unnest && l instanceof Set<?> lAsSet) {
			// TODO Should this copy be an unsafe option?
			asSet = ImmutableSet.copyOf(lAsSet);
		} else if (unnest && l instanceof Iterable<?> lAsIterable) {
			asSet = ImmutableSet.copyOf(lAsIterable);
		} else {
			asSet = Set.of(l);
		}
		return asSet;
	}

	protected Set<?> aggregateSets(Set<?> l, Set<?> r) {
		return UnionSetAggregation.<Object>unionSet(l, r);
	}

	/**
	 * This is useful to be used in {@link Map#}.merge
	 * 
	 * @param <T>
	 * @param left
	 * @param right
	 * @return
	 */
	public static <T> Set<T> unionSet(Set<? extends T> left, Set<? extends T> right) {
		if (left == null) {
			if (right == null) {
				return ImmutableSet.of();
			} else {
				return ImmutableSet.copyOf(right);
			}
		} else if (right == null) {
			return ImmutableSet.copyOf(left);
		} else {
			return ImmutableSet.<T>builder().addAll(left).addAll(right).build();
		}
	}
}

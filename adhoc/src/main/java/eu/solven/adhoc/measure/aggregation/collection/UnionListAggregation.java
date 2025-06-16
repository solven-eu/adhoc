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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.measure.aggregation.IAggregation;

/**
 * {@link IAggregation} which essentially concatenate {@link List}.
 * 
 * @author Benoit Lacelle
 */
public class UnionListAggregation extends AUnionCollectionAggregation {

	// https://duckdb.org/docs/stable/sql/functions/aggregates#array_aggarg
	// TODO Rename into ARRAY_AGG? ARRAY_AGG Would also handles arrays and Collections
	public static final String KEY = "UNION_LIST";

	public UnionListAggregation() {
		super();
	}

	public UnionListAggregation(Map<String, ?> options) {
		super(options);
	}

	public boolean acceptAggregate(Object o) {
		return o instanceof List || o == null;
	}

	@Override
	@SuppressWarnings("PMD.ReturnEmptyCollectionRatherThanNull")
	protected List<?> onEmpty() {
		return null;
	}

	@Override
	public List<?> aggregate(Object l, Object r) {
		List<?> lAsList = wrapAsList(l);
		List<?> rAsList = wrapAsList(r);

		List<?> aggregated = aggregateLists(lAsList, rAsList);

		if (aggregated.isEmpty()) {
			return onEmpty();
		}
		return aggregated;
	}

	protected List<?> wrapAsList(Object l) {
		if (l == null) {
			return List.of();
		} else if (unnest && l instanceof List<?> list) {
			// TODO Should this copy be an unsafe option?
			return ImmutableList.copyOf(list);
		} else if (unnest && l instanceof Collection<?> collection) {
			return ImmutableList.copyOf(collection);
		} else {
			return ImmutableList.of(l);
		}
	}

	protected List<?> aggregateLists(List<?> l, List<?> r) {
		return UnionListAggregation.<Object>unionList(l, r);
	}

	/**
	 * This is useful to be used in {@link Map#merge}
	 * 
	 * @param <K>
	 * @param left
	 * @param right
	 * @return
	 */
	private static <K> List<? extends K> unionList(List<? extends K> left, List<? extends K> right) {
		if (left == null || left.isEmpty()) {
			return right;
		} else if (right == null || right.isEmpty()) {
			return left;
		} else {
			return ImmutableList.<K>builder().addAll(left).addAll(right).build();
		}
	}

}

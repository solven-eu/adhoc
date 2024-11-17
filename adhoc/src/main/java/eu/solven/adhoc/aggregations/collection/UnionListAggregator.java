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

	@Override
	public double aggregateDoubles(double left, double right) {
		throw new UnsupportedOperationException("Can not %s on doubles".formatted(KEY));
	}

	@Override
	public long aggregateLongs(long left, long right) {
		throw new UnsupportedOperationException("Can not %s on longs".formatted(KEY));
	}

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

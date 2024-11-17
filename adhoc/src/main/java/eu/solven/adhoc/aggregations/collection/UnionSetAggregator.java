package eu.solven.adhoc.aggregations.collection;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.aggregations.IAggregation;

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
	 * @param <T>
	 * @param left
	 * @param right
	 * @return
	 */
	public static <T> Set<T> unionSet(Set<T> left, Set<T> right) {
		return new UnionSetAggregator<T>().aggregateSets(left, right);
	}
}

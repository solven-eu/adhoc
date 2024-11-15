package eu.solven.adhoc.aggregations;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

public class MapAggregator<K, V> implements IAggregation {

	public static final String KEY = "UNION";

	@Override
	public Map<K, V> aggregate(Object l, Object r) {
		Map<?, ?> lAsMap = (Map<?, ?>) l;
		Map<?, ?> rAsMap = (Map<?, ?>) r;

		if (l == null) {
			return (Map<K, V>) rAsMap;
		} else if (r == null) {
			return (Map<K, V>) lAsMap;
		} else {
			return (Map<K, V>) ImmutableMap.builder().putAll(lAsMap).putAll(rAsMap).build();
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
}

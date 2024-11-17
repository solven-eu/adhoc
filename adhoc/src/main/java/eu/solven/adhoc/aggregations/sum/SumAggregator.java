package eu.solven.adhoc.aggregations.sum;

import eu.solven.adhoc.aggregations.IAggregation;

public class SumAggregator implements IAggregation {

	public static final String KEY = "SUM";

	@Override
	public Object aggregate(Object l, Object r) {
		if (l == null) {
			return r;
		} else if (r == null) {
			return l;
		} else if (isLongLike(l) && isLongLike(r)) {
			return ((Number) l).longValue() + ((Number) r).longValue();
		} else {
			return ((Number) l).doubleValue() + ((Number) r).doubleValue();
		}
	}

	@Override
	public double aggregateDoubles(double left, double right) {
		return left + right;
	}

	@Override
	public long aggregateLongs(long left, long right) {
		return left + right;
	}

	public static boolean isLongLike(Object o) {
		return Integer.class.isInstance(o) || Long.class.isInstance(o);
	}
}

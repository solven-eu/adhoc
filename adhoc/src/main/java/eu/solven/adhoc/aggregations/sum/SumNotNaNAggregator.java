package eu.solven.adhoc.aggregations.sum;

import eu.solven.adhoc.aggregations.IAggregation;

/**
 * Like {@link SumAggregator}, but ignores {@link Double#NaN}
 * 
 * @author Benoit Lacelle
 *
 */
public class SumNotNaNAggregator implements IAggregation {

	public static final String KEY = "SUM";

	@Override
	public Object aggregate(Object l, Object r) {
		if (l == null) {
			return r;
		} else if (r == null) {
			return l;
		} else if (isLongLike(l) && isLongLike(r)) {
			return aggregateLongs(((Number) l).longValue(), ((Number) r).longValue());
		} else {
			return aggregateDoubles(((Number) l).doubleValue(), ((Number) r).doubleValue());
		}
	}

	@Override
	public double aggregateDoubles(double l, double r) {
		if (Double.isNaN(l)) {
			if (Double.isNaN(r)) {
				return 0D;
			} else {
				return r;
			}
		} else if (Double.isNaN(r)) {
			return l;
		} else {
			return l + r;
		}
	}

	@Override
	public long aggregateLongs(long l, long r) {
		return l + r;
	}

	public static boolean isLongLike(Object o) {
		return Integer.class.isInstance(o) || Long.class.isInstance(o);
	}
}

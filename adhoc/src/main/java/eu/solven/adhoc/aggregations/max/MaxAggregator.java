package eu.solven.adhoc.aggregations.max;

import eu.solven.adhoc.aggregations.IAggregation;

public class MaxAggregator implements IAggregation {

	public static final String KEY = "MAX";

	@Override
	public Object aggregate(Object l, Object r) {
		if (l == null) {
			return r;
		} else if (r == null) {
			return l;
		} else {
			double leftAsDouble = ((Number) l).doubleValue();
			double rightAsDouble = ((Number) r).doubleValue();

			if (leftAsDouble > rightAsDouble) {
				return l;
			} else {
				return r;
			}
		}
	}

	@Override
	public double aggregateDoubles(double left, double right) {
		return Double.max(left, right);
	}

	@Override
	public long aggregateLongs(long left, long right) {
		return Long.max(left, right);
	}
}

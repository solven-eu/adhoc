package eu.solven.adhoc.aggregations;

public class UnionAggregator implements IAggregation {

	public static final String KEY = "UNION";

	private final MapAggregator<Object, Object> mapAggregator = new MapAggregator<>();
	private final UnionSetAggregator<Object> setAggregator = new UnionSetAggregator<>();

	@Override
	public Object aggregate(Object l, Object r) {
		if (mapAggregator.acceptAggregate(l) && mapAggregator.acceptAggregate(r)) {
			return mapAggregator.aggregate(l, r);
		} else if (setAggregator.acceptAggregate(l) && setAggregator.acceptAggregate(r)) {
			return setAggregator.aggregate(l, r);
		} else {
			throw new IllegalArgumentException("No strategy in %s to merge %s and %s".formatted(KEY, l, r));
		}
	}

	@Override
	public double aggregateDoubles(double l, double r) {
		if (setAggregator.acceptAggregate(l) && setAggregator.acceptAggregate(r)) {
			return setAggregator.aggregateDoubles(l, r);
		} else {
			throw new IllegalArgumentException("No strategy in %s to merge %s and %s".formatted(KEY, l, r));
		}
	}

	@Override
	public long aggregateLongs(long l, long r) {
		if (setAggregator.acceptAggregate(l) && setAggregator.acceptAggregate(r)) {
			return setAggregator.aggregateLongs(l, r);
		} else {
			throw new IllegalArgumentException("No strategy in %s to merge %s and %s".formatted(KEY, l, r));
		}
	}
}

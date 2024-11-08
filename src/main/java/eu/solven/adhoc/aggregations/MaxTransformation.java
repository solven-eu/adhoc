package eu.solven.adhoc.aggregations;

import java.util.List;

public class MaxTransformation implements ITransformation {

	public static final String KEY = "MAX";

	final IAggregation agg = new MaxAggregator();

	@Override
	public Object transform(List<Object> underlyingValues) {
		return underlyingValues.stream().filter(o -> o != null).reduce(null, agg::aggregate);
	}

}

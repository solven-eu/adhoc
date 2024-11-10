package eu.solven.adhoc.aggregations;

import java.util.List;

public class MaxTransformation implements ITransformation {

	public static final String KEY = "MAX";

	final IAggregation agg = new MaxAggregator();

	@Override
	public Object transform(List<?> underlyingValues) {
		return underlyingValues.stream().filter(o -> o != null).<Object>map(o -> o).reduce(null, agg::aggregate);
	}

}

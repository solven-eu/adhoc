package eu.solven.adhoc.aggregations;

import java.util.List;

public class SumTransformation implements ITransformation {

	public static final String KEY = "SUM";

	final IAggregation agg = new SumAggregator();

	@Override
	public Object transform(List<?> underlyingValues) {
		return underlyingValues.stream().filter(o -> o != null).<Object>map(o -> o).reduce(null, agg::aggregate);
	}

}

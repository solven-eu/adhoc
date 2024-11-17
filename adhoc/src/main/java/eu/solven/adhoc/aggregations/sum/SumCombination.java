package eu.solven.adhoc.aggregations.sum;

import java.util.List;

import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.ICombination;

public class SumCombination implements ICombination {

	public static final String KEY = "SUM";

	final IAggregation agg = new SumAggregator();

	@Override
	public Object combine(List<?> underlyingValues) {
		return underlyingValues.stream().filter(o -> o != null).<Object>map(o -> o).reduce(null, agg::aggregate);
	}

}

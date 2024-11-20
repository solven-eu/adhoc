package eu.solven.adhoc.aggregations.max;

import java.util.List;
import java.util.Objects;

import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.ICombination;

public class MaxTransformation implements ICombination {

	public static final String KEY = "MAX";

	final IAggregation agg = new MaxAggregator();

	@Override
	public Object combine(List<?> underlyingValues) {
		return underlyingValues.stream().filter(Objects::nonNull).<Object>map(o -> o).reduce(null, agg::aggregate);
	}

}

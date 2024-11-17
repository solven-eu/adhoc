package eu.solven.adhoc.aggregations;

import java.util.List;

// https://learn.microsoft.com/en-us/dax/divide-function-dax
public class DivideTransformation implements ITransformation {

	public static final String KEY = "DIVIDE";

	@Override
	public Object transform(List<?> underlyingValues) {
		if (underlyingValues.size() != 2) {
			throw new IllegalArgumentException("Expected 2 underlyings. Got %s".formatted(underlyingValues.size()));
		}

		if (underlyingValues.get(0) instanceof Number numerator) {
			if (underlyingValues.get(1) instanceof Number denominator) {
				return numerator.doubleValue() / denominator.doubleValue();
			} else {
				return Double.NaN;
			}
		} else {
			return Double.NaN;
		}
	}

}

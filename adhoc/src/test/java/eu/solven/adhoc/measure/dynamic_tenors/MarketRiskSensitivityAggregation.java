package eu.solven.adhoc.measure.dynamic_tenors;

import eu.solven.adhoc.measure.aggregation.IAggregation;

public class MarketRiskSensitivityAggregation implements IAggregation {

	@Override
	public Object aggregate(Object left, Object right) {
		if (left == null) {
			return right;
		} else if (right == null) {
			return left;
		} else {
			return ((MarketRiskSensitivity) left).mergeWith((MarketRiskSensitivity) right);
		}
	}

}

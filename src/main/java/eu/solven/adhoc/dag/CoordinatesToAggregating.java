package eu.solven.adhoc.dag;

import java.util.Map;

import eu.solven.adhoc.storage.AggregatingMeasurators2;

public class CoordinatesToAggregating {
	final AggregatingMeasurators2<Map<String, ?>> coordinateToValue = new AggregatingMeasurators2<>();
}

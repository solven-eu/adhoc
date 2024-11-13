package eu.solven.adhoc.storage;

import java.util.HashMap;
import java.util.Map;

import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.ITransformationFactory;
import eu.solven.adhoc.transformers.Aggregator;
import lombok.Value;

@Value
public class AggregatingMeasurators<T> {

	Map<Aggregator, MultiTypeStorage<T>> aggregatorToStorage = new HashMap<>();

	ITransformationFactory transformationFactory;

	public void contribute(Aggregator aggregator, T key, Object v) {
		MultiTypeStorage<T> storage =
				aggregatorToStorage.computeIfAbsent(aggregator, k -> MultiTypeStorage.<T>builder().build());
		String aggregationKey = aggregator.getAggregationKey();
		IAggregation agg = transformationFactory.makeAggregation(aggregationKey);

		storage.merge(key, v, agg);
	}

	public long size(Aggregator aggregator) {
		MultiTypeStorage<T> storage = aggregatorToStorage.get(aggregator);
		if (storage == null) {
			return 0L;
		} else {
			return storage.size();
		}
	}
}

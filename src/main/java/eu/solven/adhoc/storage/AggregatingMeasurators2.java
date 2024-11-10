package eu.solven.adhoc.storage;

import java.util.HashMap;
import java.util.Map;

import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.dag.DAG;
import eu.solven.adhoc.transformers.Aggregator;
import lombok.Value;

@Value
public class AggregatingMeasurators2<T> {

	Map<Aggregator, MultiTypeStorage<T>> aggregatorToStorage = new HashMap<>();

	public void contribute(Aggregator aggregator, T key, Object v) {
		MultiTypeStorage<T> storage = aggregatorToStorage.computeIfAbsent(aggregator, k -> new MultiTypeStorage<T>());
		String aggregationKey = aggregator.getAggregationKey();
		IAggregation agg = DAG.makeAggregation(aggregationKey);

		storage.merge(key, v, agg);
	}

//	public void onValue(T aggregator, ValueConsumer consumer) {
//		if (measureToAggregateD.containsKey(aggregator)) {
//
//			if (measureToAggregateL.containsKey(aggregator)) {
//				// both double and long
//				double asDouble = 0D;
//
//				asDouble += measureToAggregateD.getDouble(aggregator);
//				asDouble += measureToAggregateL.getLong(aggregator);
//
//				consumer.onDouble(asDouble);
//			} else {
//				consumer.onDouble(measureToAggregateD.getDouble(aggregator));
//			}
//		} else if (measureToAggregateL.containsKey(aggregator)) {
//			consumer.onLong(measureToAggregateL.getLong(aggregator));
//		} else {
//			// unknown
//		}
//	}

	public long size(Aggregator aggregator) {
		MultiTypeStorage<T> storage = aggregatorToStorage.get(aggregator);
		if (storage == null) {
			return 0L;
		} else {
			return storage.size();
		}
	}
}

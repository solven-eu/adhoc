package eu.solven.adhoc.storage;

import eu.solven.adhoc.transformers.Combinator;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import lombok.Value;

@Value
public class CollectingCombinators implements IResultCollector {

	Object2DoubleMap<Combinator> measureToAggregateD = new Object2DoubleOpenHashMap<>();
	Object2LongMap<Combinator> measureToAggregateL = new Object2LongOpenHashMap<>();

	public void contribute(Combinator measure, Object v) {
		if (v instanceof Double || v instanceof Float) {
			double vAsPrimitive = ((Number) v).doubleValue();
			measureToAggregateD.put(measure, vAsPrimitive);
		} else if (v instanceof Long || v instanceof Integer) {
			long vAsPrimitive = ((Number) v).longValue();
			measureToAggregateL.put(measure, vAsPrimitive);
		} else {
			throw new UnsupportedOperationException("Received: %s".formatted(v));
		}
	}

	public void onValue(Combinator aggregator, ValueConsumer consumer) {
		if (measureToAggregateD.containsKey(aggregator)) {

			if (measureToAggregateL.containsKey(aggregator)) {
				// both double and long
				double asDouble = 0D;

				asDouble += measureToAggregateD.getDouble(aggregator);
				asDouble += measureToAggregateL.getLong(aggregator);

				consumer.onDouble(asDouble);
			} else {
				consumer.onDouble(measureToAggregateD.getDouble(aggregator));
			}
		} else if (measureToAggregateL.containsKey(aggregator)) {
			consumer.onLong(measureToAggregateL.getLong(aggregator));
		} else {
			// unknown
		}
	}

	public long size(Combinator aggregator) {
		long size = 0;

		if (measureToAggregateD.containsKey(aggregator)) {
			size++;
		}
		if (measureToAggregateL.containsKey(aggregator)) {
			size++;
		}

		return size;
	}
}

package eu.solven.adhoc.storage;

import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.dag.DAG;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.IMeasure;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import lombok.Value;

@Deprecated
@Value
public class AggregatingMeasurators {

	Object2DoubleMap<Aggregator> measureToAggregateD = new Object2DoubleOpenHashMap<>();
	Object2LongMap<Aggregator> measureToAggregateL = new Object2LongOpenHashMap<>();

	public void contribute(Aggregator measure, Object v) {
		IAggregation agg = DAG.makeAggregation(measure.getAggregationKey());

		if (v instanceof Double || v instanceof Float) {
			double vAsPrimitive = ((Number) v).doubleValue();
			measureToAggregateD.mergeDouble(measure, vAsPrimitive, agg::aggregateDoubles);
		} else if (v instanceof Long || v instanceof Integer) {
			long vAsPrimitive = ((Number) v).longValue();
			measureToAggregateL.mergeLong(measure, vAsPrimitive, agg::aggregateLongs);
		} else {
			throw new UnsupportedOperationException("Received: %s".formatted(v));
		}
	}

	public void onValue(IMeasure aggregator, ValueConsumer consumer) {
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

	public long size(IMeasure aggregator) {
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

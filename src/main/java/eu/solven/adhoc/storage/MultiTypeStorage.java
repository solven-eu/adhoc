package eu.solven.adhoc.storage;

import java.util.HashSet;
import java.util.Set;

import eu.solven.adhoc.RowScanner;
import eu.solven.adhoc.aggregations.IAggregation;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

public class MultiTypeStorage<T> {

	Object2DoubleMap<T> measureToAggregateD = new Object2DoubleOpenHashMap<>();
	Object2LongMap<T> measureToAggregateL = new Object2LongOpenHashMap<>();

	public void put(T key, Object v) {

		if (v instanceof Double || v instanceof Float) {
			double vAsPrimitive = ((Number) v).doubleValue();
			measureToAggregateD.put(key, vAsPrimitive);
		} else if (v instanceof Long || v instanceof Integer) {
			long vAsPrimitive = ((Number) v).longValue();
			measureToAggregateL.put(key, vAsPrimitive);
		} else {
			throw new UnsupportedOperationException("Received: %s".formatted(v));
		}
	}

	public void onValue(T key, ValueConsumer consumer) {
		if (measureToAggregateD.containsKey(key)) {

			if (measureToAggregateL.containsKey(key)) {
				// both double and long
				double asDouble = 0D;

				asDouble += measureToAggregateD.getDouble(key);
				asDouble += measureToAggregateL.getLong(key);

				consumer.onDouble(asDouble);
			} else {
				consumer.onDouble(measureToAggregateD.getDouble(key));
			}
		} else if (measureToAggregateL.containsKey(key)) {
			consumer.onLong(measureToAggregateL.getLong(key));
		} else {
			// unknown
		}
	}

	public void scan(RowScanner <T>rowScanner) {
		keySet().forEach(key -> {
			onValue(key, rowScanner.onKey(key));
		});
	}

	public long size() {
		long size = 0;

		size += measureToAggregateD.size();
		size += measureToAggregateL.size();

		return size;
	}

	public void merge(T key, Object v, IAggregation agg) {
		if (v instanceof Double || v instanceof Float) {
			double vAsPrimitive = ((Number) v).doubleValue();
			measureToAggregateD.mergeDouble(key, vAsPrimitive, agg::aggregateDoubles);
		} else if (v instanceof Long || v instanceof Integer) {
			long vAsPrimitive = ((Number) v).longValue();
			measureToAggregateL.mergeLong(key, vAsPrimitive, agg::aggregateLongs);
		} else {
			throw new UnsupportedOperationException("Received: %s".formatted(v));
		}
	}

	public Set<T> keySet() {
		Set<T> keySet = new HashSet<>();

		keySet.addAll(measureToAggregateD.keySet());
		keySet.addAll(measureToAggregateL.keySet());

		return keySet;
	}
}

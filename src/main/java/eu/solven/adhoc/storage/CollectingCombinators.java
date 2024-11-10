package eu.solven.adhoc.storage;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

@Deprecated
@Value
@Builder
public class CollectingCombinators<T> {

	@Default
	MultiTypeStorage<T> storage = new MultiTypeStorage<T>();

	public void contribute(T key, Object v) {
		storage.put(key, v);
	}

	public void onValue(T aggregator, ValueConsumer consumer) {
		storage.onValue(aggregator, consumer);
	}

	public long size() {
		return storage.size();
	}
}

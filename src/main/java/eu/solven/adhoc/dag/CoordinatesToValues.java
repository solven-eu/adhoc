package eu.solven.adhoc.dag;

import java.util.Map;

import eu.solven.adhoc.RowScanner;
import eu.solven.adhoc.storage.MultiTypeStorage;
import eu.solven.adhoc.storage.ValueConsumer;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

@Value
@Builder
public class CoordinatesToValues {
	@Default
	MultiTypeStorage<Map<String, ?>> storage = new MultiTypeStorage<>();

	public static CoordinatesToValues empty() {
		return CoordinatesToValues.builder().build();
	}

	public void onValue(Map<String, ?> coordinate, ValueConsumer consumer) {
		storage.onValue(coordinate, consumer);
	}

	public void put(Map<String, ?> coordinate, Object value) {
		storage.put(coordinate, value);
	}

	public void scan(RowScanner<Map<String, ?>> rowScanner) {
		storage.scan(rowScanner);
	}
}

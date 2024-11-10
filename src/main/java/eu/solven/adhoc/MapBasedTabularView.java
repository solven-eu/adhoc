package eu.solven.adhoc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import eu.solven.adhoc.aggregations.MapAggregator;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import eu.solven.adhoc.storage.ValueConsumer;
import lombok.Builder;
import lombok.Builder.Default;

@Builder
public class MapBasedTabularView implements ITabularView {
	@Default
	final Map<Map<String, ?>, Map<String, ?>> coordinatesToValues = new HashMap<>();

	public static MapBasedTabularView load(ITabularView output) {
		MapBasedTabularView newView = MapBasedTabularView.builder().build();

		RowScanner<Map<String, ?>> rowScanner = new RowScanner<Map<String, ?>>() {

			@Override
			public void onRow(Map<String, ?> coordinates, Map<String, ?> values) {
				newView.coordinatesToValues.put(coordinates, values);
			}

			@Override
			public ValueConsumer onKey(Map<String, ?> coordinates) {
				return AsObjectValueConsumer.consumer(o -> {
					newView.coordinatesToValues.computeIfAbsent(coordinates, k -> new HashMap<>()).put(null, null);
				});
			}
		};

		output.acceptScanner(rowScanner);

		return newView;
	}

	@Override
	public Stream<Map<String, ?>> keySet() {
		return coordinatesToValues.keySet().stream();
	}

	@Override
	public void acceptScanner(RowScanner<Map<String, ?>> rowScanner) {
		coordinatesToValues.forEach(rowScanner::onRow);
	}

	public void append(Map<String, ?> coordinates, Map<String, ?> mToValues) {
		// if (coordinatesToValues.containsKey(coordinates)) {
		// throw new IllegalArgumentException("There is already some values for " + coordinates);
		// }
		coordinatesToValues.merge(coordinates, mToValues, new MapAggregator<String, Object>()::aggregate);

	}

	public static ITabularView empty() {
		return MapBasedTabularView.builder().coordinatesToValues(Collections.emptyMap()).build();
	}
}

package eu.solven.adhoc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

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

			// @Override
			// public void onRow(Map<String, ?> coordinates, Map<String, ?> values) {
			// newView.coordinatesToValues.put(coordinates, values);
			// }

			@Override
			public ValueConsumer onKey(Map<String, ?> coordinates) {
				if (newView.coordinatesToValues.containsKey(coordinates)) {
					throw new IllegalArgumentException("Already has value for %s".formatted(coordinates));
				}

				return AsObjectValueConsumer.consumer(o -> {
					Map<String, ?> oAsMap = (Map<String, ?>) o;

					newView.coordinatesToValues.put(coordinates, oAsMap);
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
		coordinatesToValues.forEach((k, v) -> {
			rowScanner.onKey(k).onObject(v);
		});
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

	@Override
	public String toString() {
		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", coordinatesToValues.size());

		AtomicInteger index = new AtomicInteger();
		coordinatesToValues.entrySet()
				.stream()
				.limit(5)
				.forEach(entry -> toStringHelper.add("#" + index.getAndIncrement(), entry));

		return toStringHelper.toString();
	}
}

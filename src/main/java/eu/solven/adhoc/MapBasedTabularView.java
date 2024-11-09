package eu.solven.adhoc;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import eu.solven.adhoc.aggregations.MapAggregator;

public class MapBasedTabularView implements ITabularView {
	final Map<Map<String, ?>, Map<String, ?>> coordinatesToValues = new HashMap<>();

	public static MapBasedTabularView load(ITabularView output) {
		MapBasedTabularView newView = new MapBasedTabularView();

		output.acceptScanner((coordinates, values) -> {
			newView.coordinatesToValues.put(coordinates, values);
		});

		return newView;
	}

	@Override
	public Stream<Map<String, ?>> keySet() {
		return coordinatesToValues.keySet().stream();
	}

	@Override
	public void acceptScanner(RowScanner rowScanner) {
		coordinatesToValues.forEach(rowScanner::onRow);
	}

	public void append(Map<String, ?> coordinates, Map<String, ?> mToValues) {
		// if (coordinatesToValues.containsKey(coordinates)) {
		// throw new IllegalArgumentException("There is already some values for " + coordinates);
		// }
		coordinatesToValues.merge(coordinates, mToValues, new MapAggregator<String, Object>()::aggregate);

	}
}

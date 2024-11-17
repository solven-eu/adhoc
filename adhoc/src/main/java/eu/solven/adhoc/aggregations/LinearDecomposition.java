package eu.solven.adhoc.aggregations;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import eu.solven.adhoc.api.v1.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.query.GroupByColumns;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.pepper.mappath.MapPathGet;

public class LinearDecomposition implements IDecomposition {
	public static final String KEY = "linear";

	final Map<String, ?> options;

	public LinearDecomposition(Map<String, ?> options) {
		this.options = options;
	}

	@Override
	public Map<Map<String, ?>, Object> decompose(Map<String, ?> coordinate, Object value) {
		String inputColumn = MapPathGet.getRequiredString(options, "input");

		Optional<?> optInput = MapPathGet.getOptionalAs(coordinate, inputColumn);
		if (optInput.isEmpty()) {
			return Map.of(Map.of(), value);
		}

		Object input = optInput.get();
		Number min = MapPathGet.getRequiredNumber(options, "min");
		Number max = MapPathGet.getRequiredNumber(options, "max");

		String outputColumn = MapPathGet.getRequiredString(options, "output");
		if (min.equals(input)) {
			return Collections.singletonMap(Map.of(outputColumn, min), value);
		} else if (max.equals(input)) {
			return Collections.singletonMap(Map.of(outputColumn, max), value);
		} else {
			Map<Map<String, ?>, Object> output = new HashMap<>();

			output.put(Map.of(outputColumn, min), scale(min, max, input, value));
			output.put(Map.of(outputColumn, max), scaleComplement(min, max, input, value));

			return output;
		}
	}

	private Object scale(Number min, Number max, Object input, Object value) {
		if (input instanceof Number inputAsNumber) {
			if (inputAsNumber.doubleValue() <= min.doubleValue()) {
				// input is min, so we return nothing from value
				return 0D;
			} else if (inputAsNumber.doubleValue() >= max.doubleValue()) {
				// input is min, so we return all from value
				return value;
			} else {
				if (value instanceof Number valueAsNumber) {
					double fraction = inputAsNumber.doubleValue() - min.doubleValue();
					double denominator = max.doubleValue() - min.doubleValue();

					return valueAsNumber.doubleValue() * fraction / denominator;
				} else {
					return Double.NaN;
				}
			}
		} else {
			return Double.NaN;
		}
	}

	private Object scaleComplement(Number min, Number max, Object input, Object value) {
		Object scaled = scale(min, max, input, value);

		if (scaled instanceof Double scaledAsNumber) {
			if (Double.isNaN(scaledAsNumber.doubleValue())) {
				return Double.NaN;
			}

			if (value instanceof Number valueAsNumber) {
				return valueAsNumber.doubleValue() - scaledAsNumber.doubleValue();
			} else {
				return Double.NaN;
			}
		} else {
			return Double.NaN;
		}
	}

	@Override
	public List<IWhereGroupbyAdhocQuery> getUnderlyingSteps(AdhocQueryStep step) {
		String outputColumn = MapPathGet.getRequiredString(options, "output");
		if (!step.getGroupBy().getGroupedByColumns().contains(outputColumn)) {
			// None of the requested column is an output column of this dispatchor : there is nothing to dispatch
			return Collections.singletonList(step);
		}

		// If we are requested on the dispatched level, we have to groupBy the input level
		Set<String> allGroupBys = new HashSet<>();
		allGroupBys.addAll(step.getGroupBy().getGroupedByColumns());
		allGroupBys.remove(outputColumn);

		String inputColumn = MapPathGet.getRequiredString(options, "input");
		allGroupBys.add(inputColumn);

		return Collections.singletonList(
				MeasurelessQuery.builder().filter(step.getFilter()).groupBy(GroupByColumns.of(allGroupBys)).build());
	}

}

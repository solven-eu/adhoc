/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
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
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.groupby.IAdhocColumn;
import eu.solven.adhoc.query.groupby.ReferencedColumn;
import eu.solven.pepper.mappath.MapPathGet;

public class LinearDecomposition implements IDecomposition {
	public static final String KEY = "linear";
	/**
	 * The column used as input, expected to be between min and max.
	 */
	public static final String K_INPUT = "input";
	/**
	 * The column written by this decomposition.
	 */
	public static final String K_OUTPUT = "output";

	final Map<String, ?> options;

	public LinearDecomposition(Map<String, ?> options) {
		this.options = options;
	}

	@Override
	public Map<Map<String, ?>, Object> decompose(Map<String, ?> coordinate, Object value) {
		String inputColumn = MapPathGet.getRequiredString(options, K_INPUT);

		Optional<?> optInput = MapPathGet.getOptionalAs(coordinate, inputColumn);
		if (optInput.isEmpty()) {
			return Map.of(Map.of(), value);
		}

		Object input = optInput.get();
		Number min = MapPathGet.getRequiredNumber(options, "min");
		Number max = MapPathGet.getRequiredNumber(options, "max");

		String outputColumn = MapPathGet.getRequiredString(options, K_OUTPUT);
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
		Set<IAdhocColumn> allGroupBys = new HashSet<>();
		allGroupBys.addAll(step.getGroupBy().getNameToColumn().values());
		allGroupBys.removeIf(c -> c.getColumn().equals(outputColumn));

		String inputColumn = MapPathGet.getRequiredString(options, K_INPUT);
		allGroupBys.add(ReferencedColumn.ref(inputColumn));

		return Collections.singletonList(
				MeasurelessQuery.builder().filter(step.getFilter()).groupBy(GroupByColumns.of(allGroupBys)).build());
	}

}

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
package eu.solven.adhoc.measure.decomposition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.pepper.mappath.MapPathGet;

/**
 * This is an example {@link IDecomposition}, which takes the value in the input column, and split the underlying
 * measure intosome min and max value to the output column.
 * <p>
 * For instance, given v=200 on input=13 with min=0 and max=100, we write 26 into output=0 and 174 into output=100.
 * 
 * @author Benoit Lacelle
 */
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
	public List<IDecompositionEntry> decompose(ISliceWithStep slice, Object value) {
		String inputColumn = MapPathGet.getRequiredString(options, K_INPUT);

		Optional<?> optInput = slice.getSlice().optSliced(inputColumn);
		if (optInput.isEmpty()) {
			return List.of(IDecompositionEntry.of(Map.of(), IValueProvider.setValue(value)));
		}

		Object input = optInput.get();
		Number min = MapPathGet.getRequiredNumber(options, "min");
		Number max = MapPathGet.getRequiredNumber(options, "max");

		String outputColumn = MapPathGet.getRequiredString(options, K_OUTPUT);
		if (min.equals(input)) {
			return List.of(IDecompositionEntry.of(Map.of(outputColumn, min), value));
		} else if (max.equals(input)) {
			return List.of(IDecompositionEntry.of(Map.of(outputColumn, max), value));
		} else {
			List<IDecompositionEntry> output = new ArrayList<>(2);

			output.add(IDecompositionEntry.of(Map.of(outputColumn, min), scale(min, max, input, value)));
			output.add(IDecompositionEntry.of(Map.of(outputColumn, max), scaleComplement(min, max, input, value)));

			return output;
		}
	}

	protected Object scale(Number min, Number max, Object input, Object value) {
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

	@SuppressWarnings("PMD.UnnecessaryBoxing")
	protected Object scaleComplement(Number min, Number max, Object input, Object value) {
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
	public List<IWhereGroupByQuery> getUnderlyingSteps(CubeQueryStep step) {
		String outputColumn = MapPathGet.getRequiredString(options, K_OUTPUT);
		if (!step.getGroupBy().getGroupedByColumns().contains(outputColumn)) {
			// None of the requested column is an output column of this dispatchor : there is nothing to dispatch
			return Collections.singletonList(step);
		}

		// If we are requested on the dispatched level, we have to groupBy the input level
		Set<IAdhocColumn> allGroupBys = new HashSet<>();
		allGroupBys.addAll(step.getGroupBy().getNameToColumn().values());
		allGroupBys.removeIf(c -> c.getName().equals(outputColumn));

		String inputColumn = MapPathGet.getRequiredString(options, K_INPUT);
		allGroupBys.add(ReferencedColumn.ref(inputColumn));

		return Collections.singletonList(MeasurelessQuery.edit(step).groupBy(GroupByColumns.of(allGroupBys)).build());
	}

	@Override
	public Map<String, Class<?>> getColumnTypes() {
		return ImmutableMap.<String, Class<?>>builder()
				.put(MapPathGet.getRequiredString(options, K_OUTPUT), Number.class)
				.build();
	}

	@Override
	public CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		Number min = MapPathGet.getRequiredNumber(options, "min");
		Number max = MapPathGet.getRequiredNumber(options, "max");

		return CoordinatesSample.builder().estimatedCardinality(2).coordinate(min).coordinate(max).build();
	}

}

/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.table.duckdb.var;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.decomposition.IDecompositionEntry;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.value.IValueMatcher;

/**
 * This will handle transforming the underlying array into individual array elements if `scenarioIndex` column is
 * expressed.
 * 
 * @author Benoit Lacelle
 */
public class ExampleVaRDecomposition implements IDecomposition, IExampleVaRConstants {

	protected final int nbScenarios;

	public ExampleVaRDecomposition(Map<String, ?> options) {
		Object rawNbScenarios = options.get(NB_SCENARIO);
		nbScenarios = ((Number) rawNbScenarios).intValue();
	}

	@Override
	public List<IWhereGroupByQuery> getUnderlyingSteps(CubeQueryStep step) {
		MeasurelessQuery suppressedIndex = IDecomposition.suppressColumn(step, C_SCENARIOINDEX);
		MeasurelessQuery suppressedName = IDecomposition.suppressColumn(suppressedIndex, C_SCENARIONAME);
		return Collections.singletonList(suppressedName);
	}

	@Override
	public List<IDecompositionEntry> decompose(ISliceWithStep slice, Object value) {
		NavigableSet<String> groupedByColumns = slice.getQueryStep().getGroupBy().getGroupedByColumns();
		boolean groupByScenario =
				groupedByColumns.contains(C_SCENARIOINDEX) || groupedByColumns.contains(C_SCENARIONAME);

		IValueMatcher scenarioIndexMatcher = FilterHelpers.getValueMatcher(slice.asFilter(), C_SCENARIOINDEX);
		IValueMatcher scenarioNameMatcher = FilterHelpers.getValueMatcher(slice.asFilter(), C_SCENARIONAME);

		if (groupByScenario) {
			// Decompose by scenario: will return a value instead of an array

			int[] array = (int[]) value;

			List<IDecompositionEntry> decomposition = new ArrayList<>(nbScenarios);

			IntStream.range(0, nbScenarios)
					.filter(scenario -> scenarioIndexMatcher.match(scenario)
							&& scenarioNameMatcher.match(ExampleVaRScenarioNameCombination.indexToName(scenario)))
					.forEach(scenario -> {
						ImmutableMap<String, Object> scenarioSlice = ImmutableMap.<String, Object>builder()
								.put(C_SCENARIOINDEX, scenario)
								.put(C_SCENARIONAME, ExampleVaRScenarioNameCombination.indexToName(scenario))
								.build();
						decomposition.add(IDecompositionEntry.of(scenarioSlice, array[scenario]));
					});

			return decomposition;
		} else {
			// Return an array, but possibly only a subset of scenarios

			Set<String> filteredColumns = FilterHelpers.getFilteredColumns(slice.asFilter());
			if (!filteredColumns.contains(C_SCENARIOINDEX) && !filteredColumns.contains(C_SCENARIONAME)) {
				// No filter on scenarios
				return List.of(IDecompositionEntry.of(Map.of(), value));
			} else {
				int[] array = (int[]) value;

				int[] filteredValuesAsArray = IntStream.range(0, nbScenarios)
						.filter(scenario -> scenarioIndexMatcher.match(scenario)
								&& scenarioNameMatcher.match(ExampleVaRScenarioNameCombination.indexToName(scenario)))
						.map(scenario -> array[scenario])
						.toArray();

				// Return a single slice, with the selected subset of scenarios
				return List.of(IDecompositionEntry.of(Map.of(), filteredValuesAsArray));
			}
		}
	}

	@Override
	public Map<String, Class<?>> getColumnTypes() {
		return ImmutableMap.<String, Class<?>>builder()
				.put(C_SCENARIOINDEX, Integer.class)
				.put(C_SCENARIONAME, String.class)
				.build();
	}

	@Override
	public CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		if (C_SCENARIOINDEX.equals(column)) {
			return CoordinatesSample.builder()
					.coordinates(IntStream.range(0, nbScenarios).mapToObj(i -> i).toList())
					.estimatedCardinality(nbScenarios)
					.build();
		} else if (C_SCENARIONAME.equals(column)) {
			return CoordinatesSample.builder()
					.coordinates(IntStream.range(0, nbScenarios)
							.mapToObj(i -> ExampleVaRScenarioNameCombination.indexToName(i))
							.toList())
					.estimatedCardinality(nbScenarios)
					.build();
		} else {
			return CoordinatesSample.empty();
		}
	}

}

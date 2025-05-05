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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.MeasurelessQuery.MeasurelessQueryBuilder;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;

/**
 * This will handle transforming the underlying array into individual array elements if `scenarioIndex` column is
 * expressed.
 * 
 * @author Benoit Lacelle
 */
public class ExampleVaRDecomposition implements IDecomposition {

	protected final int nbScenarios;

	public ExampleVaRDecomposition(Map<String, ?> options) {
		Object rawNbScenarios = options.get(IExampleVaRConstants.NB_SCENARIO);
		nbScenarios = ((Number) rawNbScenarios).intValue();
	}

	@Override
	public List<IWhereGroupByQuery> getUnderlyingSteps(CubeQueryStep step) {
		MeasurelessQuery suppressedIndex = suppressColumn(step, IExampleVaRConstants.C_SCENARIOINDEX);
		MeasurelessQuery suppressedName = suppressColumn(suppressedIndex, IExampleVaRConstants.C_SCENARIONAME);
		return Collections.singletonList(suppressedName);
	}

	public static MeasurelessQuery suppressColumn(IWhereGroupByQuery step, String column) {
		MeasurelessQueryBuilder underlyingStep = MeasurelessQuery.edit(step);

		if (step.getGroupBy().getGroupedByColumns().contains(column)) {
			// Underlying measure handles an array: `scenarioIndex` is meaningless
			Map<String, IAdhocColumn> groupByWithoutIndex = new LinkedHashMap<>(step.getGroupBy().getNameToColumn());
			groupByWithoutIndex.remove(column);
			underlyingStep.groupBy(GroupByColumns.of(groupByWithoutIndex.values())).build();
		}

		if (FilterHelpers.getFilteredColumns(step.getFilter()).contains(column)) {
			// Underlying measure handles an array: `scenarioIndex` is meaningless
			IAdhocFilter supressedFilter = SimpleFilterEditor.suppressColumn(step.getFilter(), Set.of(column));

			underlyingStep.filter(supressedFilter);
			// BEWARE In a different design, we should ensure we query only the relevant underlying double columns.
			// This is not done here as it would require more coupled logics
		}

		return underlyingStep.build();
	}

	@Override
	public Map<Map<String, ?>, Object> decompose(ISliceWithStep slice, Object value) {
		NavigableSet<String> groupedByColumns = slice.getQueryStep().getGroupBy().getGroupedByColumns();
		boolean groupByScenario = groupedByColumns.contains(IExampleVaRConstants.C_SCENARIOINDEX)
				|| groupedByColumns.contains(IExampleVaRConstants.C_SCENARIONAME);

		IValueMatcher scenarioIndexMatcher =
				FilterHelpers.getValueMatcher(slice.asFilter(), IExampleVaRConstants.C_SCENARIOINDEX);
		IValueMatcher scenarioNameMatcher =
				FilterHelpers.getValueMatcher(slice.asFilter(), IExampleVaRConstants.C_SCENARIONAME);

		if (groupByScenario) {
			// Decompose by scenario: will return a value instead of an array

			int[] array = (int[]) value;

			Map<Map<String, ?>, Object> decomposition = new LinkedHashMap<>();

			IntStream.range(0, nbScenarios)
					.filter(scenario -> scenarioIndexMatcher.match(scenario)
							&& scenarioNameMatcher.match(ExampleVaRScenarioNameCombination.indexToName(scenario)))
					.forEach(scenario -> {
						decomposition.put(ImmutableMap.<String, Object>builder()
								.put(IExampleVaRConstants.C_SCENARIOINDEX, scenario)
								.put(IExampleVaRConstants.C_SCENARIONAME,
										ExampleVaRScenarioNameCombination.indexToName(scenario))
								.build(), array[scenario]);
					});

			return decomposition;
		} else {
			// Return an array, but possibly only a subset of scenarios

			Set<String> filteredColumns = FilterHelpers.getFilteredColumns(slice.asFilter());
			if (!filteredColumns.contains(IExampleVaRConstants.C_SCENARIOINDEX)
					&& !filteredColumns.contains(IExampleVaRConstants.C_SCENARIONAME)) {
				// No filter on scenarios
				return Map.of(Map.of(), value);
			} else {
				int[] array = (int[]) value;

				int[] filteredValuesAsArray = IntStream.range(0, nbScenarios)
						.filter(scenario -> scenarioIndexMatcher.match(scenario)
								&& scenarioNameMatcher.match(ExampleVaRScenarioNameCombination.indexToName(scenario)))
						.map(scenario -> array[scenario])
						.toArray();

				// Return a single slice, with the selected subset of scenarios
				return Map.of(Map.of(), filteredValuesAsArray);
			}
		}
	}

	@Override
	public Map<String, Class<?>> getColumnTypes() {
		return ImmutableMap.<String, Class<?>>builder()
				.put(IExampleVaRConstants.C_SCENARIOINDEX, Integer.class)
				.put(IExampleVaRConstants.C_SCENARIONAME, String.class)
				.build();
	}

	@Override
	public CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		if (IExampleVaRConstants.C_SCENARIOINDEX.equals(column)) {
			return CoordinatesSample.builder()
					.coordinates(IntStream.range(0, nbScenarios).mapToObj(i -> i).toList())
					.estimatedCardinality(nbScenarios)
					.build();
		} else if (IExampleVaRConstants.C_SCENARIONAME.equals(column)) {
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

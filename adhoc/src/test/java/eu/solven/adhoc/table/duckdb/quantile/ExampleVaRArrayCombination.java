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
package eu.solven.adhoc.table.duckdb.quantile;

import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.dag.step.ISliceWithStep;
import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.transformator.column_generator.IColumnGenerator;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.pepper.core.PepperLogHelper;

/**
 * Collecting the N underlying measures into a single array. This is due to most `table` not enabling aggregation over
 * arrays.
 * 
 * @author Benoit Lacelle
 */
public class ExampleVaRArrayCombination implements ICombination, IColumnGenerator {

	public static final String KEY = "ARRAY";

	protected final int nbScenarios;

	public ExampleVaRArrayCombination(Map<String, ?> options) {
		Object rawNbScenarios = options.get(IExampleVaRConstants.NB_SCENARIO);
		nbScenarios = ((Number) rawNbScenarios).intValue();
	}

	@Override
	public IValueProvider combine(ISliceWithStep slice, ISlicedRecord slicedRecord) {
		int size = slicedRecord.size();

		// ExampleVaR: storing into a int[] as it is easier to register unitTests (SUM aggregations are predictable,
		// while double-SUM may lead to precision issues for unitTests).
		int[] valuesAsArray = new int[size];

		for (int i = 0; i < size; i++) {
			int finalI = i;

			slicedRecord.read(i, new IValueReceiver() {

				@Override
				public void onLong(long v) {
					valuesAsArray[finalI] = (int) v;
				}

				@Override
				public void onDouble(double v) {
					valuesAsArray[finalI] = (int) v;
				}

				@Override
				public void onObject(Object v) {
					throw new IllegalArgumentException(
							"Unexpected type. Received %s".formatted(PepperLogHelper.getObjectAndClass(v)));
				}
			});
		}

		if (FilterHelpers.getFilteredColumns(slice.asFilter()).contains(IExampleVaRConstants.C_SCENARIOINDEX)) {
			// TODO This should be propagated into the ITransformator as we seemingly computed all indexes while only a
			// subset were requested
			IValueMatcher valueMatcher =
					FilterHelpers.getValueMatcher(slice.asFilter(), IExampleVaRConstants.C_SCENARIOINDEX);

			int[] filteredValuesAsArray = IntStream.range(0, nbScenarios)
					.filter(i -> valueMatcher.match(i))
					.map(filteredIndex -> valuesAsArray[filteredIndex])
					.toArray();

			return vr -> vr.onObject(filteredValuesAsArray);
		} else {
			return vr -> vr.onObject(valuesAsArray);
		}

	}

	@Override
	public Set<String> getOutputColumns() {
		return Set.of(IExampleVaRConstants.C_SCENARIOINDEX);
	}

	@Override
	public CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		return CoordinatesSample.builder()
				.coordinates(IntStream.range(0, nbScenarios).mapToObj(i -> i).toList())
				.estimatedCardinality(nbScenarios)
				.build();
	}

}

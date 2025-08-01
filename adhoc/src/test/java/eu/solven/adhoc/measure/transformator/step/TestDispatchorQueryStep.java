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
package eu.solven.adhoc.measure.transformator.step;

import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.column.FunctionCalculatedColumn;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.engine.step.SliceAsMapWithStep;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestDispatchorQueryStep {
	private boolean isRelevant(Map<String, String> decompositionSLice, ISliceFilter stepFilter) {
		DispatchorQueryStep step = step(stepFilter);

		return step.isRelevant(decompositionSLice);
	}

	private DispatchorQueryStep step(ISliceFilter stepFilter) {
		Dispatchor d = Dispatchor.builder().name("d").underlying("u").build();
		CubeQueryStep cubeStep = CubeQueryStep.builder().measure("d").filter(stepFilter).build();
		DispatchorQueryStep step = new DispatchorQueryStep(d, AdhocFactories.builder().build(), cubeStep);
		return step;
	}

	@Test
	public void testMatchDecompositionEntry_noFilter() {
		Assertions.assertThat(isRelevant(Map.of("c", "v"), AndFilter.and(Map.of()))).isTrue();
	}

	@Test
	public void testMatchDecompositionEntry_exactMatch() {
		Assertions.assertThat(isRelevant(Map.of("c", "v"), AndFilter.and(Map.of("c", "v")))).isTrue();
	}

	@Test
	public void testMatchDecompositionEntry_exactMatch_decompositeMoreColumns() {
		Assertions.assertThat(isRelevant(Map.of("c", "v", "c2", "v2"), AndFilter.and(Map.of("c", "v")))).isTrue();
	}

	@Test
	public void testMatchDecompositionEntry_filterMore() {
		Assertions.assertThat(isRelevant(Map.of("c", "v"), AndFilter.and(Map.of("c", "v", "c2", "v2")))).isTrue();
	}

	@Test
	public void testMatchDecompositionEntry_notRelevant() {
		Assertions.assertThat(isRelevant(Map.of("c", "v"), AndFilter.and(Map.of("c", "v2")))).isFalse();
	}

	@Test
	public void testMatchDecompositionEntry_in() {
		Assertions.assertThat(isRelevant(Map.of("c", "v"), AndFilter.and(Map.of("c", Set.of("v", "v2"))))).isTrue();
	}

	@Test
	public void testCalculatedColumn() {
		ISliceFilter stepFilter = ISliceFilter.MATCH_ALL;
		CubeQueryStep cubeStep = CubeQueryStep.builder().measure("d").filter(stepFilter).build();

		IAdhocGroupBy groupBy = GroupByColumns.of(FunctionCalculatedColumn.builder()
				.name("computedC")
				.recordToCoordinate(r -> r.getGroupBy("underlyingC") + "_post")
				.build());
		ISliceWithStep sliceWithStep = SliceAsMapWithStep.builder()
				.queryStep(cubeStep)
				.slice(SliceAsMap.fromMap(Map.of("underlyingC", "underlyingV")))
				.build();
		IAdhocSlice o = step(stepFilter).queryGroupBy(groupBy, sliceWithStep, Map.of());

		Assertions.assertThat((Map) o.getCoordinates()).containsEntry("computedC", "underlyingV" + "_post");
	}

}

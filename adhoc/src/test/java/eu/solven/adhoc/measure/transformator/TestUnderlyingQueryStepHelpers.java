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
package eu.solven.adhoc.measure.transformator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.MultitypeHashColumn;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasures;

public class TestUnderlyingQueryStepHelpers {
	@Test
	public void testDistinctSlices_notOrdered_oneUnderlying() {
		CubeQueryStep queryStep = CubeQueryStep.builder().measure(Aggregator.countAsterisk()).build();
		List<ISliceToValue> underlyings = new ArrayList<>();

		{
			IMultitypeColumnFastGet<SliceAsMap> column = MultitypeHashColumn.<SliceAsMap>builder().build();
			column.append(SliceAsMap.fromMap(Map.of("c", "c1"))).onLong(123);
			column.append(SliceAsMap.fromMap(Map.of("c", "c3"))).onLong(345);
			column.append(SliceAsMap.fromMap(Map.of("c", "c2"))).onLong(234);
			underlyings.add(SliceToValue.builder().column(column).build());
		}

		List<SliceAndMeasures> slices = UnderlyingQueryStepHelpers.distinctSlices(queryStep, underlyings).toList();

		Assertions.assertThat(slices).hasSize(3).anySatisfy(slice -> {
			Assertions.assertThat(slice.getSlice().getAdhocSliceAsMap().getCoordinates()).isEqualTo(Map.of("c", "c1"));
		}).anySatisfy(slice -> {
			Assertions.assertThat(slice.getSlice().getAdhocSliceAsMap().getCoordinates()).isEqualTo(Map.of("c", "c2"));
		}).anySatisfy(slice -> {
			Assertions.assertThat(slice.getSlice().getAdhocSliceAsMap().getCoordinates()).isEqualTo(Map.of("c", "c3"));
		});
	}

	@Test
	public void testDistinctSlices_notOrdered_twoUnderlying() {
		CubeQueryStep queryStep = CubeQueryStep.builder().measure(Aggregator.countAsterisk()).build();
		List<ISliceToValue> underlyings = new ArrayList<>();

		{
			IMultitypeColumnFastGet<SliceAsMap> column = MultitypeHashColumn.<SliceAsMap>builder().build();
			column.append(SliceAsMap.fromMap(Map.of("c", "c1"))).onLong(123);
			column.append(SliceAsMap.fromMap(Map.of("c", "c3"))).onLong(345);
			column.append(SliceAsMap.fromMap(Map.of("c", "c2"))).onLong(234);
			underlyings.add(SliceToValue.builder().column(column).build());
		}
		{
			IMultitypeColumnFastGet<SliceAsMap> column = MultitypeHashColumn.<SliceAsMap>builder().build();
			column.append(SliceAsMap.fromMap(Map.of("c", "c3"))).onLong(123);
			column.append(SliceAsMap.fromMap(Map.of("c", "c5"))).onLong(345);
			column.append(SliceAsMap.fromMap(Map.of("c", "c4"))).onLong(234);
			underlyings.add(SliceToValue.builder().column(column).build());
		}

		List<SliceAndMeasures> slices = UnderlyingQueryStepHelpers.distinctSlices(queryStep, underlyings).toList();

		Assertions.assertThat(slices).hasSize(5).anySatisfy(slice -> {
			Assertions.assertThat(slice.getSlice().getAdhocSliceAsMap().getCoordinates()).isEqualTo(Map.of("c", "c1"));
		}).anySatisfy(slice -> {
			Assertions.assertThat(slice.getSlice().getAdhocSliceAsMap().getCoordinates()).isEqualTo(Map.of("c", "c2"));
		}).anySatisfy(slice -> {
			Assertions.assertThat(slice.getSlice().getAdhocSliceAsMap().getCoordinates()).isEqualTo(Map.of("c", "c3"));
		}).anySatisfy(slice -> {
			Assertions.assertThat(slice.getSlice().getAdhocSliceAsMap().getCoordinates()).isEqualTo(Map.of("c", "c4"));
		}).anySatisfy(slice -> {
			Assertions.assertThat(slice.getSlice().getAdhocSliceAsMap().getCoordinates()).isEqualTo(Map.of("c", "c5"));
		});
	}
}

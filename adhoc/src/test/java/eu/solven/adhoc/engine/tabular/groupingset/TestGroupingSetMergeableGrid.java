/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.engine.tabular.groupingset;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.dataframe.aggregating.AggregatingColumnsDistinct;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.map.SliceHelpers;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestGroupingSetMergeableGrid {
	Aggregator a = Aggregator.sum("v");

	GroupingSetMergeableGrid grid = GroupingSetMergeableGrid.builder()
			.gridFactory(() -> AggregatingColumnsDistinct.<IAdhocSlice>builder().build())
			.build();

	// Slices from two different groupBy keysets trigger two distinct underlying grids.
	@Test
	public void testTwoUnderlyingGrids() {
		IAdhocSlice sliceCountry = SliceHelpers.asSlice(Map.of("country", "FR"));
		IAdhocSlice sliceCcy = SliceHelpers.asSlice(Map.of("ccy", "EUR"));

		grid.contribute(sliceCountry, a).onLong(10);
		grid.contribute(sliceCcy, a).onLong(20);

		Assertions.assertThat(grid.getAggregators()).containsExactly("v");

		CubeQueryStep stepCountry =
				CubeQueryStep.builder().measure("m").groupBy(GroupByColumns.named("country")).build();
		CubeQueryStep stepCcy = CubeQueryStep.builder().measure("m").groupBy(GroupByColumns.named("ccy")).build();

		IMultitypeColumnFastGet<IAdhocSlice> colCountry = grid.closeColumn(stepCountry, a);
		IMultitypeColumnFastGet<IAdhocSlice> colCcy = grid.closeColumn(stepCcy, a);

		Assertions.assertThat(IValueProvider.getValue(colCountry.onValue(sliceCountry))).isEqualTo(10L);
		Assertions.assertThat(IValueProvider.getValue(colCcy.onValue(sliceCcy))).isEqualTo(20L);
	}

	// toString() must not throw even when two underlying grids are present.
	@Test
	public void testToString_twoGrids() {
		IAdhocSlice sliceCountry = SliceHelpers.asSlice(Map.of("country", "FR"));
		IAdhocSlice sliceCcy = SliceHelpers.asSlice(Map.of("ccy", "EUR"));

		grid.contribute(sliceCountry, a).onLong(10);
		grid.contribute(sliceCcy, a).onLong(20);

		Assertions.assertThatCode(() -> grid.toString()).doesNotThrowAnyException();
		Assertions.assertThat(grid.toString()).isNotEmpty();
	}

	// toString() on an empty grid (no contributions at all) must not throw.
	@Test
	public void testToString_empty() {
		Assertions.assertThatCode(() -> grid.toString()).doesNotThrowAnyException();
	}
}

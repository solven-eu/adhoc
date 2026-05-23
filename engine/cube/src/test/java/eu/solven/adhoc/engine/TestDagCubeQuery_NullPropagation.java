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
package eu.solven.adhoc.engine;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ATestDagInMemory;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.measure.sum.DivideCombination;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.model.measure.Partitionor;
import eu.solven.adhoc.model.measure.Unfiltrator;
import eu.solven.adhoc.model.query.groupby.GroupByColumns;

/**
 * Pins null-propagation semantics through a multi-underlying {@link Combinator} when the underlyings have asymmetric
 * slice coverage. The shape is the simplest version of the ratio-vs-whole pattern: one underlying is filtered, the
 * other is unfiltered, and a slice exists in the unfiltered cuboid that is missing in the filtered one.
 *
 * <p>
 * Expected behaviour: when {@link DivideCombination} returns null at a slice (per its own contract, "returning null if
 * denominator is null is a nice way to prevent Unfiltrator to materialize irrelevant slices"), the output cuboid must
 * NOT include that slice in the final tabular view. A regression here means null is being converted somewhere — likely
 * to NaN — before reaching the column's null-cleaning path.
 */
public class TestDagCubeQuery_NullPropagation extends ATestDagInMemory {

	@Override
	@BeforeEach
	public void feedTable() {
		// FR has a Paris row, DE does not.
		table().add(Map.of("country", "FR", "city", "Paris", "d", 100));
		table().add(Map.of("country", "DE", "city", "Berlin", "d", 200));
	}

	@BeforeEach
	public void registerMeasures() {
		// d_filtered: the user-filtered d. groupBy=country, filter=city=Paris → {FR=100} only.
		// d_unfiltered: same as d but with the city filter removed. groupBy=country, filter=matchAll → {FR=100,
		// DE=200}.
		// ratio: d_filtered / d_unfiltered. At DE: null / 200 → should be null per DivideCombination's contract,
		// and the slice should be omitted by the column's null cleaning.
		forest.addMeasure(Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build());
		forest.addMeasure(Combinator.builder()
				.name("d_filtered")
				.underlying("d")
				.combinationKey(CoalesceCombination.KEY)
				.build());
		forest.addMeasure(
				Unfiltrator.builder().name("d_unfiltered").underlying("d").unfilterOthersThan("country").build());
		forest.addMeasure(Combinator.builder()
				.name("ratio")
				.underlying("d_filtered")
				.underlying("d_unfiltered")
				.combinationKey(DivideCombination.KEY)
				.build());
	}

	@Test
	public void testDivideWithNullNumerator_partitionorChain_omitsSlice() {
		// Same shape as testDivideWithNullNumerator_omitsSlice but with d_filtered as a Partitionor (groupBy=country)
		// instead of a CoalesceCombinator. The Partitionor's groupBy is covered by the query's groupBy, so the
		// PartitionorToCombinatorOptimizer would in principle rewrite it. With the rewrite, the chain must still
		// omit the DE slice — surfacing a DE row (NaN or otherwise) reveals where null propagation drops.
		forest.clear();
		forest.addMeasure(Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build());
		forest.addMeasure(Partitionor.builder()
				.name("d_filtered")
				.underlying("d")
				.groupBy(GroupByColumns.named("country"))
				.combinationKey(SumCombination.KEY)
				.build());
		// d_unfiltered reads d_filtered (NOT d directly) — mirrors RatioOverCurrentColumnValueCompositor.
		forest.addMeasure(Unfiltrator.builder()
				.name("d_unfiltered")
				.underlying("d_filtered")
				.unfilterOthersThan("country")
				.build());
		forest.addMeasure(Combinator.builder()
				.name("ratio")
				.underlying("d_filtered")
				.underlying("d_unfiltered")
				.combinationKey(DivideCombination.KEY)
				.build());

		CubeQuery query =
				CubeQuery.builder().measure("ratio").groupByAlso("country").andFilter("city", "Paris").build();
		ITabularView view = cube().execute(query);
		MapBasedTabularView mapBased = MapBasedTabularView.load(view);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.as("Final view should contain only FR — even when d_filtered is a Partitionor or rewritten to Combinator")
				.hasSize(1)
				.containsKey(Map.of("country", "FR"));
	}

	@Test
	public void testDivideWithNullNumerator_omitsSlice() {
		CubeQuery query =
				CubeQuery.builder().measure("ratio").groupByAlso("country").andFilter("city", "Paris").build();
		ITabularView view = cube().execute(query);
		MapBasedTabularView mapBased = MapBasedTabularView.load(view);

		// The DE slice has d_filtered=null + d_unfiltered=200. DivideCombination returns null at DE; the cuboid
		// column's null-cleaning (CleaningValueReceiver with cleanIfNull=true) should drop the slice. So the final
		// view contains only FR. A regression that surfaces a DE row (e.g. NaN) means null became some non-null
		// value before reaching the cleaning step.
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.as("Final view should contain only FR (DE has null numerator → entire ratio slice should be omitted)")
				.hasSize(1)
				.containsKey(Map.of("country", "FR"));
	}
}

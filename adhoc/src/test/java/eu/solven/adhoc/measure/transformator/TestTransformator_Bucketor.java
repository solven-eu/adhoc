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
package eu.solven.adhoc.measure.transformator;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.aggregation.comparable.MaxCombination;
import eu.solven.adhoc.measure.model.Bucketor;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.cube.AdhocQuery;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestTransformator_Bucketor extends ADagTest implements IAdhocTestConstants {
	@Override
	@BeforeEach
	public void feedTable() {
		table.add(Map.of("a", "a1", "k1", 123));
		table.add(Map.of("a", "a1", "k1", 345, "k2", 456));
		table.add(Map.of("a", "a2", "b", "b1", "k2", 234));
		table.add(Map.of("a", "a2", "b", "b2", "k1", 567));
	}

	@Test
	public void testSumOfMaxOfSum_noGroupBy() {
		forest.addMeasure(Bucketor.builder()
				.name("maxK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(MaxCombination.KEY)
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube.execute(AdhocQuery.builder().measure("maxK1K2").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("maxK1K2", 0L + (123 + 345) + 567));
	}

	@Test
	public void testSumOfMaxOfSum_groupByA() {
		forest.addMeasure(Bucketor.builder()
				.name("maxK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.named("a"))
				.combinationKey(MaxCombination.KEY)
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube.execute(AdhocQuery.builder().measure("maxK1K2").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("maxK1K2", 0L + (123 + 345) + 567));
	}

	@Test
	public void testSumOfMaxOfSum_groupByAandB() {
		forest.addMeasure(Bucketor.builder()
				.name("maxK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.named("a", "b"))
				.combinationKey(MaxCombination.KEY)
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube.execute(AdhocQuery.builder().measure("maxK1K2").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("maxK1K2", 0L + 234 + 567));
	}

	@Test
	public void testSumOfMaxOfSum_groupByA_bothBucketorAndAdhoc() {
		forest.addMeasure(Bucketor.builder()
				.name("maxK1K2ByA")
				.underlyings(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.named("a"))
				.combinationKey(MaxCombination.KEY)
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube.execute(AdhocQuery.builder().measure("maxK1K2ByA").groupByAlso("a").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("a", "a1"), Map.of("maxK1K2ByA", 0L + 123 + 345))
				.containsEntry(Map.of("a", "a2"), Map.of("maxK1K2ByA", 0L + 567));
	}

	@Test
	public void testSumOfMaxOfSum_groupByAandBandUnknown() {
		forest.addMeasure(Bucketor.builder()
				.name("maxK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.named("a", "b", "unknownColumn"))
				.combinationKey(MaxCombination.KEY)
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		Assertions.setMaxStackTraceElementsDisplayed(300);
		Assertions.assertThatThrownBy(() -> cube.execute(AdhocQuery.builder().measure("maxK1K2").build()))
				.isInstanceOf(IllegalArgumentException.class)
				.hasStackTraceContaining("unknownColumn");
	}

	@Test
	public void testSumOfSum_filterA1_groupbyA() {
		forest.addMeasure(Bucketor.builder()
				.name("maxK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.named("a"))
				.combinationKey(MaxCombination.KEY)
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube.execute(AdhocQuery.builder().measure("maxK1K2").andFilter("a", "a1").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of("maxK1K2", 0L + 123 + 345));
	}

	@Test
	public void testSumOfSum_filterA1_groupbyB() {
		forest.addMeasure(Bucketor.builder()
				.name("maxK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.named("b"))
				.combinationKey(MaxCombination.KEY)
				.aggregationKey(SumAggregation.KEY)
				.tag("debug")
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube.execute(AdhocQuery.builder().measure("maxK1K2").andFilter("a", "a2").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of("maxK1K2", 0L + 234 + 567));
	}

}

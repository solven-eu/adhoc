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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.aggregation.comparable.MaxCombination;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.pepper.collection.MapWithNulls;

public class TestTransformator_Partitionor extends ADagTest implements IAdhocTestConstants {
	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of("a", "a1", "k1", 123));
		table().add(Map.of("a", "a1", "k1", 345, "k2", 456));
		table().add(Map.of("a", "a2", "b", "b1", "k2", 234));
		table().add(Map.of("a", "a2", "b", "b2", "k1", 567));
	}

	@Test
	public void testSumOfMaxOfSum_noGroupBy() {
		forest.addMeasure(Partitionor.builder()
				.name("maxK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(MaxCombination.KEY)
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube().execute(CubeQuery.builder().measure("maxK1K2").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("maxK1K2", 0L + (123 + 345) + 567));
	}

	@Test
	public void testSumOfMaxOfSum_partitionByA() {
		forest.addMeasure(Partitionor.builder()
				.name("maxK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.named("a"))
				.combinationKey(MaxCombination.KEY)
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube().execute(CubeQuery.builder().measure("maxK1K2").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("maxK1K2", 0L + (123 + 345) + 567));
	}

	@Test
	public void testSumOfMaxOfSum_partitionByAandB() {
		forest.addMeasure(Partitionor.builder()
				.name("maxK1K2")
				.combinationKey(MaxCombination.KEY)
				.underlyings(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.named("a", "b"))
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube().execute(CubeQuery.builder().measure("maxK1K2").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("maxK1K2", 0L + Math.max(123 + 345, 456) + 234 + 567));
	}

	@Test
	public void testSumOfMaxOfSum_partitionByAandB_groupByAandB() {
		forest.addMeasure(Partitionor.builder()
				.name("maxK1K2")
				.combinationKey(MaxCombination.KEY)
				.underlyings(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.named("a", "b"))
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output =
				cube().execute(CubeQuery.builder().measure("maxK1K2").groupBy(GroupByColumns.named("a", "b")).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(3)
				.containsEntry(MapWithNulls.of("a", "a1", "b", null), Map.of("maxK1K2", 0L + 123 + 345))
				.containsEntry(Map.of("a", "a2", "b", "b1"), Map.of("maxK1K2", 0L + 234))
				.containsEntry(Map.of("a", "a2", "b", "b2"), Map.of("maxK1K2", 0L + 567));
	}

	@Test
	public void testSumOfMaxOfSum_partitionByA_bothBucketorAndAdhoc() {
		forest.addMeasure(Partitionor.builder()
				.name("maxK1K2ByA")
				.underlyings(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.named("a"))
				.combinationKey(MaxCombination.KEY)
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube().execute(CubeQuery.builder().measure("maxK1K2ByA").groupByAlso("a").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("a", "a1"), Map.of("maxK1K2ByA", 0L + 123 + 345))
				.containsEntry(Map.of("a", "a2"), Map.of("maxK1K2ByA", 0L + 567));
	}

	@Test
	public void testSumOfMaxOfSum_partitionByAandBandUnknown() {
		forest.addMeasure(Partitionor.builder()
				.name("maxK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.named("a", "b", "unknownColumn"))
				.combinationKey(MaxCombination.KEY)
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		Assertions.setMaxStackTraceElementsDisplayed(300);
		Assertions.assertThatThrownBy(() -> cube().execute(CubeQuery.builder().measure("maxK1K2").build()))
				.isInstanceOf(IllegalArgumentException.class)
				.hasStackTraceContaining("unknownColumn");
	}

	@Test
	public void testSumOfSum_filterA1_partitionByA() {
		forest.addMeasure(Partitionor.builder()
				.name("maxK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.named("a"))
				.combinationKey(MaxCombination.KEY)
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube().execute(CubeQuery.builder().measure("maxK1K2").andFilter("a", "a1").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of("maxK1K2", 0L + 123 + 345));
	}

	/**
	 * A custom {@link ICombination} that records each {@code b} coordinate it observes via
	 * {@code sliceReader().extractCoordinateLax()}, so the test can assert that a null {@code b} value is handled
	 * gracefully (i.e. {@link Optional#empty()} for the partition where {@code b} is absent).
	 *
	 * <p>
	 * The static list is cleared at the start of each test that uses this class.
	 *
	 * <p>
	 * See docs/combination.md — "The GROUP BY guarantee" section for a full explanation of the NullMatcher case.
	 */
	public static class RecordingBCombination implements ICombination {

		// package-private for test assertion; cleared per-test
		static final List<Optional<String>> seenBValues = new ArrayList<>();

		@Override
		public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
			// b is in the Partitionor groupBy — may be null for rows where b is absent in the source data
			Optional<String> b = slice.sliceReader().extractCoordinateLax("b", String.class);
			seenBValues.add(b);
			return underlyingValues.get(0); // pass through
		}
	}

	/**
	 * Verifies that a custom {@link ICombination} using {@code sliceReader().extractCoordinateLax()} correctly handles
	 * a {@code null} coordinate in the Partitionor GROUP BY. Column {@code b} is absent (hence null) for rows where
	 * {@code a=a1}, so the combination must receive {@link Optional#empty()} for those slices rather than throwing.
	 *
	 * <p>
	 * See docs/combination.md — "The GROUP BY guarantee" section.
	 */
	@Test
	public void testCombination_nullCoordinate_inPartitionorGroupBy() {
		RecordingBCombination.seenBValues.clear();

		forest.addMeasure(Partitionor.builder()
				.name("k1ByB")
				.underlyings(Arrays.asList("k1"))
				.groupBy(GroupByColumns.named("b"))
				.combinationKey(RecordingBCombination.class.getName())
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);

		ITabularView output = cube().execute(CubeQuery.builder().measure("k1ByB").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		// Grand-total result: all partitions re-aggregated to a single cell
		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).containsKey(Collections.emptyMap());

		// The null-b partition (rows with a=a1, where b is absent) produces Optional.empty()
		Assertions.assertThat(RecordingBCombination.seenBValues).contains(Optional.empty());

		// Non-null partitions produce the expected coordinate values
		Assertions.assertThat(RecordingBCombination.seenBValues)
				.contains(Optional.of("b1"))
				.contains(Optional.of("b2"));
	}

	@Test
	public void testSumOfSum_filterA1_partitionByB() {
		forest.addMeasure(Partitionor.builder()
				.name("maxK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.named("b"))
				.combinationKey(MaxCombination.KEY)
				.aggregationKey(SumAggregation.KEY)
				.tag("debug")
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube().execute(CubeQuery.builder().measure("maxK1K2").andFilter("a", "a2").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of("maxK1K2", 0L + 234 + 567));
	}

}

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
package eu.solven.adhoc.query;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.ratio.AdhocExplainerTestHelper;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.query.cube.AdhocQuery;
import eu.solven.adhoc.util.IStopwatch;

public class TestAggregations_Filter extends ADagTest implements IAdhocTestConstants {
	@Override
	@BeforeEach
	public void feedTable() {
		table.add(Map.of("a", "a1", "k1", 123));
		table.add(Map.of("a", "a2", "b", "b1", "k2", 234));
		table.add(Map.of("a", "a1", "k1", 345, "k2", 456));
		table.add(Map.of("a", "a2", "b", "b2", "k1", 567));
	}

	@Test
	public void testSumOfSum_filterA1() {
		forest.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube.execute(AdhocQuery.builder().measure("sumK1K2").andFilter("a", "a1").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 0L + 123 + 345 + 456));
	}

	@Test
	public void testSumOfSum_filterA1_groupbyA() {
		forest.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output =
				cube.execute(AdhocQuery.builder().measure("sumK1K2").andFilter("a", "a1").groupByAlso("a").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of("a", "a1"), Map.of("sumK1K2", 0L + 123 + 345 + 456));
	}

	@Test
	public void testSumOfSum_filterA1_groupbyB() {
		forest.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output =
				cube.execute(AdhocQuery.builder().measure("sumK1K2").andFilter("a", "a2").groupByAlso("b").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("b", "b1"), Map.of("sumK1K2", 0L + 234))
				.containsEntry(Map.of("b", "b2"), Map.of("sumK1K2", 0L + 567));
	}

	@Test
	public void testSumOfSum_filterMatchNothing() {
		forest.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube.execute(AdhocQuery.builder().measure("sumK1K2").andFilter("a", "none").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(0);
	}

	@Override
	public IStopwatch makeStopwatch() {
		return () -> Duration.ofMillis(123);
	}

	@Test
	public void testLogs() {
		List<String> messages = AdhocExplainerTestHelper.listenForLogs(eventBus);

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		cube.execute(AdhocQuery.builder().measure(k1Sum.getName()).andFilter("a", "a1").build());

		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n")))
				.isEqualTo(
						"""
								Executing on table=inMemory measures=TestAggregations_Filter query=AdhocQuery(filter=a=a1, groupBy=grandTotal, measures=[ReferencedMeasure(ref=k1)], customMarker=null, debug=false, explain=false)
								Executed status=OK duration=PT0.123S on table=inMemory measures=TestAggregations_Filter query=AdhocQuery(filter=a=a1, groupBy=grandTotal, measures=[ReferencedMeasure(ref=k1)], customMarker=null, debug=false, explain=false)
																"""
								.trim());

		Assertions.assertThat(messages).hasSize(2);
	}
}

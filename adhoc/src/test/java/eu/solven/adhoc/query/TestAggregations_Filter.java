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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.measure.step.Aggregator;
import eu.solven.adhoc.measure.step.Combinator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.storage.MapBasedTabularView;

public class TestAggregations_Filter extends ADagTest {
	@Override
	@BeforeEach
	public void feedTable() {
		rows.add(Map.of("a", "a1", "k1", 123));
		rows.add(Map.of("a", "a2", "b", "b1", "k2", 234));
		rows.add(Map.of("a", "a1", "k1", 345, "k2", 456));
		rows.add(Map.of("a", "a2", "b", "b2", "k1", 567));
	}

	@Test
	public void testSumOfSum_filterA1() {
		amb.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregation.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregation.KEY).build());

		ITabularView output = aqw.execute(AdhocQuery.builder().measure("sumK1K2").andFilter("a", "a1").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 0L + 123 + 345 + 456));
	}

	@Test
	public void testSumOfSum_filterA1_groupbyA() {
		amb.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregation.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregation.KEY).build());

		ITabularView output =
				aqw.execute(AdhocQuery.builder().measure("sumK1K2").andFilter("a", "a1").groupByAlso("a").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of("a", "a1"), Map.of("sumK1K2", 0L + 123 + 345 + 456));
	}

	@Test
	public void testSumOfSum_filterA1_groupbyB() {
		amb.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregation.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregation.KEY).build());

		ITabularView output =
				aqw.execute(AdhocQuery.builder().measure("sumK1K2").andFilter("a", "a2").groupByAlso("b").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("b", "b1"), Map.of("sumK1K2", 0L + 234))
				.containsEntry(Map.of("b", "b2"), Map.of("sumK1K2", 0L + 567));
	}

	@Test
	public void testSumOfSum_filterMatchNothing() {
		amb.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregation.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregation.KEY).build());

		ITabularView output = aqw.execute(AdhocQuery.builder().measure("sumK1K2").andFilter("a", "none").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(0);
	}

}

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
package eu.solven.adhoc.measure;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MaxCombination;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.storage.MapBasedTabularView;

public class TestTransformator_Combinator_Double extends ADagTest implements IAdhocTestConstants {
	@Override
	@BeforeEach
	public void feedTable() {
		rows.add(Map.of("k1", 123D));
		rows.add(Map.of("k2", 234D));
		rows.add(Map.of("k1", 345F, "k2", 456F));
	}

	@Test
	public void testSumOfSum() {
		amb.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		amb.addMeasure(k1Sum);
		amb.addMeasure(k2Sum);

		ITabularView output = aqw.execute(AdhocQuery.builder().measure("sumK1K2").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						// "k1", 123 + 345, "k2", 234 + 456,
						Map.of("sumK1K2", 0D + 123 + 234 + 345 + 456));
	}

	@Test
	public void testSumOfMax() {
		amb.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(MaxAggregation.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(MaxAggregation.KEY).build());

		ITabularView output = aqw.execute(AdhocQuery.builder().measure("sumK1K2").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						// "k1", 345, "k2", 456,
						Map.of("sumK1K2", 0D + 345 + 456));
	}

	@Test
	public void testMaxOfSum() {
		amb.addMeasure(Combinator.builder()
				.name("maxK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(MaxCombination.KEY)
				.build());

		amb.addMeasure(k1Sum);
		amb.addMeasure(k2Sum);

		ITabularView output = aqw.execute(AdhocQuery.builder().measure("maxK1K2").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						// "k1", 123 + 345, "k2", 234 + 456,
						Map.of("maxK1K2", 0D + 234 + 456));
	}
}

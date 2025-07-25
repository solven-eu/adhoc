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
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MaxCombination;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.measure.transformator.MapWithNulls;
import eu.solven.adhoc.query.cube.CubeQuery;

public class TestAggregations_GroupBys extends ADagTest implements IAdhocTestConstants {

	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of("a", "a1", "k1", 123));
		table().add(Map.of("a", "a2", "b", "b1", "k2", 234));
		table().add(Map.of("a", "a1", "k1", 345, "k2", 456));
		table().add(Map.of("a", "a2", "b", "b2", "k1", 567));
	}

	@Test
	public void testSumOfSum_noGroupBy() {
		forest.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube().execute(CubeQuery.builder().measure("sumK1K2").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 0L + 123 + 234 + 345 + 456 + 567));
	}

	@Test
	public void testSumOfSum_groupBy1String() {
		forest.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube().execute(CubeQuery.builder().measure("sumK1K2").groupByAlso("a").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("a", "a1"), Map.of("sumK1K2", 0L + 123 + 345 + 456))
				.containsEntry(Map.of("a", "a2"), Map.of("sumK1K2", 0L + 234 + 567));
	}

	@Test
	public void testSumOfSum_groupBy1String_notAlwaysPresent() {
		forest.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube().execute(CubeQuery.builder().measure("sumK1K2").groupByAlso("b").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("b", "b1"), Map.of("sumK1K2", 0L + 234))
				.containsEntry(Map.of("b", "b2"), Map.of("sumK1K2", 0L + 567))
				.containsEntry(MapWithNulls.of("b", null), Map.of("sumK1K2", 0L + 123 + 345 + 456))
				.hasSize(3);
	}

	@Test
	public void testSumOfMax_groupBy1String() {
		forest.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		forest.addMeasure(Aggregator.builder().name("k1").aggregationKey(MaxAggregation.KEY).build());
		forest.addMeasure(Aggregator.builder().name("k2").aggregationKey(MaxAggregation.KEY).build());

		ITabularView output = cube().execute(CubeQuery.builder().measure("sumK1K2").groupByAlso("a").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("a", "a1"), Map.of("sumK1K2", 0L + 345 + 456))
				.containsEntry(Map.of("a", "a2"), Map.of("sumK1K2", 0L + 567 + 234));
	}

	@Test
	public void testMaxOfSum() {
		forest.addMeasure(Combinator.builder()
				.name("maxK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(MaxCombination.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube().execute(CubeQuery.builder().measure("maxK1K2").groupByAlso("a").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("a", "a1"), Map.of("maxK1K2", 0L + 123 + 345))
				.containsEntry(Map.of("a", "a2"), Map.of("maxK1K2", 0L + 567));
	}
}

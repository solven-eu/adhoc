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
import eu.solven.adhoc.measure.aggregation.comparable.MinAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.RankAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.CountAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.AdhocQuery;

public class TestTransformator_Aggregator extends ADagTest implements IAdhocTestConstants {
	@Override
	@BeforeEach
	public void feedTable() {
		rows.add(Map.of("a", "a1", "k1", 123));
		rows.add(Map.of("a", "a1", "k1", 345, "k2", 456));
		rows.add(Map.of("a", "a2", "b", "b1", "k2", 234));
		rows.add(Map.of("a", "a2", "b", "b2", "k1", 567));

		// This first `k1` overlaps with the columnName
		amb.addMeasure(Aggregator.builder().name("k1").columnName("k1").aggregationKey(SumAggregation.KEY).build());
		// This second `k1.SUM` does not overlap with the columnName
		amb.addMeasure(Aggregator.builder().name("k1.sum").columnName("k1").aggregationKey(SumAggregation.KEY).build());

		amb.addMeasure(Aggregator.builder().name("k1.min").columnName("k1").aggregationKey(MinAggregation.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k1.max").columnName("k1").aggregationKey(MaxAggregation.KEY).build());
		amb.addMeasure(
				Aggregator.builder().name("k1.count").columnName("k1").aggregationKey(CountAggregation.KEY).build());
	}

	@Test
	public void testK1MinMax() {
		ITabularView output = aqw.execute(AdhocQuery.builder().measure("k1", "k1.min", "k1.max").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of("k1", 0L + (123 + 345) + 567, "k1.min", 0L + 123, "k1.max", 0L + 567));
	}

	@Test
	public void testK1_SUM_COUNT() {
		ITabularView output = aqw.execute(AdhocQuery.builder().measure("k1", "k1.count").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("k1", 0L + (123 + 345) + 567, "k1.count", 0L + 3));
	}

	@Test
	public void testCountAsterisk() {
		Aggregator m = Aggregator.countAsterisk();
		amb.addMeasure(m);

		ITabularView output = aqw.execute(AdhocQuery.builder().measure(m).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(m.getName(), 0L + 4));
	}

	@Test
	public void testNoMeasure() {
		ITabularView output = aqw.execute(AdhocQuery.builder().debug(true).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of());
	}

	@Test
	public void testNoMeasureGroupBy() {
		ITabularView output = aqw.execute(AdhocQuery.builder().groupByAlso("a").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("a", "a1"), Map.of())
				.containsEntry(Map.of("a", "a2"), Map.of());
	}

	@Test
	public void testDifferingColumnName() {
		Aggregator m = Aggregator.builder().name("niceName").columnName("k1").build();
		amb.addMeasure(m);

		ITabularView output = aqw.execute(AdhocQuery.builder().measure("niceName").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(m.getName(), 0L + (123 + 345) + 567));
	}

	@Test
	public void testRank2() {
		Aggregator m = Aggregator.builder()
				.name("rank2")
				.aggregationKey(RankAggregation.KEY)
				.aggregationOption(RankAggregation.P_RANK, 2)
				.columnName("k1")
				.build();
		amb.addMeasure(m);

		ITabularView output = aqw.execute(AdhocQuery.builder().measure("rank2").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(m.getName(), 0L + 345));
	}
}

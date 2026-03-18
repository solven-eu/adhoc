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
package eu.solven.adhoc.query.table;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSetMultimap;

import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestTableQueryV4 {

	final FilteredAggregator aggSum = FilteredAggregator.builder().aggregator(Aggregator.sum("price")).build();
	final FilteredAggregator aggCount = FilteredAggregator.builder().aggregator(Aggregator.countAsterisk()).build();

	final IGroupBy gbCountry = GroupByColumns.named("country");
	final IGroupBy gbCity = GroupByColumns.named("city");

	// --- streamV3 ---

	@Test
	public void streamV3_singleGroupBy_singleAgg() {
		TableQueryV4 v4 =
				TableQueryV4.builder().groupByToAggregators(ImmutableSetMultimap.of(gbCountry, aggSum)).build();

		List<TableQueryV3> v3s = v4.streamV3().toList();

		Assertions.assertThat(v3s).hasSize(1);
		Assertions.assertThat(v3s.getFirst().getGroupBys()).containsExactlyInAnyOrder(gbCountry);
		Assertions.assertThat(v3s.getFirst().getAggregators()).containsExactlyInAnyOrder(aggSum);
	}

	@Test
	public void streamV3_identicalAggSets_collapseIntoGroupingSet() {
		// Both groupBys need the same aggregators → should produce one V3 with GROUPING SET
		TableQueryV4 v4 = TableQueryV4.builder()
				.groupByToAggregators(ImmutableSetMultimap.of(gbCountry, aggSum, gbCity, aggSum))
				.build();

		List<TableQueryV3> v3s = v4.streamV3().toList();

		Assertions.assertThat(v3s).hasSize(1);
		Assertions.assertThat(v3s.getFirst().getGroupBys()).containsExactlyInAnyOrder(gbCountry, gbCity);
		Assertions.assertThat(v3s.getFirst().getAggregators()).containsExactlyInAnyOrder(aggSum);
	}

	@Test
	public void streamV3_differentAggSets_produceTwoV3s() {
		// Each groupBy needs a different aggregator → two separate V3s (UNION ALL)
		TableQueryV4 v4 = TableQueryV4.builder()
				.groupByToAggregators(ImmutableSetMultimap.of(gbCountry, aggSum, gbCity, aggCount))
				.build();

		List<TableQueryV3> v3s = v4.streamV3().toList();

		Assertions.assertThat(v3s).hasSize(2);
		// One V3 per groupBy with its own aggregator
		Assertions.assertThat(v3s).anySatisfy(v3 -> {
			Assertions.assertThat(v3.getGroupBys()).containsExactlyInAnyOrder(gbCountry);
			Assertions.assertThat(v3.getAggregators()).containsExactlyInAnyOrder(aggSum);
		});
		Assertions.assertThat(v3s).anySatisfy(v3 -> {
			Assertions.assertThat(v3.getGroupBys()).containsExactlyInAnyOrder(gbCity);
			Assertions.assertThat(v3.getAggregators()).containsExactlyInAnyOrder(aggCount);
		});
	}

	@Test
	public void streamV3_grandTotal_singleV3() {
		TableQueryV4 v4 = TableQueryV4.builder()
				.groupByToAggregators(ImmutableSetMultimap.of(IGroupBy.GRAND_TOTAL, aggSum))
				.build();

		List<TableQueryV3> v3s = v4.streamV3().toList();

		Assertions.assertThat(v3s).hasSize(1);
		Assertions.assertThat(v3s.getFirst().streamGroupBy().toList()).containsExactly(IGroupBy.GRAND_TOTAL);
	}
}

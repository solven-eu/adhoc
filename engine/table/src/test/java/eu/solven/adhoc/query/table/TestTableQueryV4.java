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
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSetMultimap;

import eu.solven.adhoc.filter.ColumnFilter;
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
				.groupByToAggregator(gbCountry, aggSum)
				.groupByToAggregator(gbCity, aggSum)
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
				.groupByToAggregator(gbCountry, aggSum)
				.groupByToAggregator(gbCity, aggCount)
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

	// --- isPerfectV3 ---

	@Test
	public void isPerfectV3_singleGroupBy() {
		TableQueryV4 v4 =
				TableQueryV4.builder().groupByToAggregators(ImmutableSetMultimap.of(gbCountry, aggSum)).build();

		Assertions.assertThat(v4.isPerfectV3()).isTrue();
	}

	@Test
	public void isPerfectV3_sameAggsOnBothGroupBys() {
		TableQueryV4 v4 = TableQueryV4.builder()
				.groupByToAggregator(gbCountry, aggSum)
				.groupByToAggregator(gbCity, aggSum)
				.build();

		Assertions.assertThat(v4.isPerfectV3()).isTrue();
	}

	@Test
	public void isPerfectV3_differentAggsOnEachGroupBy() {
		TableQueryV4 v4 = TableQueryV4.builder()
				.groupByToAggregator(gbCountry, aggSum)
				.groupByToAggregator(gbCity, aggCount)
				.build();

		Assertions.assertThat(v4.isPerfectV3()).isFalse();
	}

	// --- asCoveringV3 ---

	@Test
	public void asCoveringV3_singleGroupBy_singleAgg() {
		TableQueryV4 v4 =
				TableQueryV4.builder().groupByToAggregators(ImmutableSetMultimap.of(gbCountry, aggSum)).build();

		TableQueryV3 v3 = v4.asCoveringV3();

		Assertions.assertThat(v3.getGroupBys()).containsExactlyInAnyOrder(gbCountry);
		Assertions.assertThat(v3.getAggregators()).containsExactlyInAnyOrder(aggSum);
	}

	@Test
	public void asCoveringV3_multipleGroupBys_sameAgg_noDuplication() {
		// Both groupBys need the same aggregator → asCoveringV3 must not duplicate it
		TableQueryV4 v4 = TableQueryV4.builder()
				.groupByToAggregators(ImmutableSetMultimap.of(gbCountry, aggSum, gbCity, aggSum))
				.build();

		TableQueryV3 v3 = v4.asCoveringV3();

		Assertions.assertThat(v3.getGroupBys()).containsExactlyInAnyOrder(gbCountry, gbCity);
		Assertions.assertThat(v3.getAggregators()).containsExactlyInAnyOrder(aggSum);
	}

	@Test
	public void asCoveringV3_sameAggNameDifferentFilter_uniqueAliases() {
		// country → sum("price" WHERE color=blue) (per-groupBy alias: "price")
		// city → sum("price" WHERE color=red) (per-groupBy alias: "price" — collision in covering V3)
		FilteredAggregator aggSumBlue = FilteredAggregator.builder()
				.aggregator(Aggregator.sum("price"))
				.filter(ColumnFilter.matchEq("color", "blue"))
				.build();
		FilteredAggregator aggSumRed = FilteredAggregator.builder()
				.aggregator(Aggregator.sum("price"))
				.filter(ColumnFilter.matchEq("color", "red"))
				.build();

		TableQueryV4 v4 = TableQueryV4.builder()
				.groupByToAggregator(gbCountry, aggSumBlue)
				.groupByToAggregator(gbCity, aggSumRed)
				.build();

		TableQueryV3 v3 = v4.asCoveringV3();

		Assertions.assertThat(v3.getGroupBys()).containsExactlyInAnyOrder(gbCountry, gbCity);
		// Both distinct aggregators must be present with distinct aliases
		Assertions.assertThat(v3.getAggregators()).hasSize(2);
		Set<String> aliases =
				v3.getAggregators().stream().map(FilteredAggregator::getAlias).collect(Collectors.toSet());
		Assertions.assertThat(aliases).hasSize(2);
	}

	@Test
	public void asCoveringV3_equivalentAggDifferentIndex_mergedIntoOne() {
		// country → sum("price" WHERE color=blue, index=0) alias "price"
		// city → sum("price" WHERE color=blue, index=1) alias "price_1" (same logical agg, different index)
		// asCoveringV3 must normalise index and emit only one aggregator
		FilteredAggregator aggIdx0 = FilteredAggregator.builder()
				.aggregator(Aggregator.sum("price"))
				.filter(ColumnFilter.matchEq("color", "blue"))
				.build();
		FilteredAggregator aggIdx1 = aggIdx0.toBuilder().index(1).build();

		TableQueryV4 v4 = TableQueryV4.builder()
				.groupByToAggregators(ImmutableSetMultimap.of(gbCountry, aggIdx0, gbCity, aggIdx1))
				.build();

		TableQueryV3 v3 = v4.asCoveringV3();

		Assertions.assertThat(v3.getGroupBys()).containsExactlyInAnyOrder(gbCountry, gbCity);
		// Logically equivalent aggregators must be merged into one
		Assertions.assertThat(v3.getAggregators()).hasSize(1);
		Assertions.assertThat(v3.getAggregators().stream().map(FilteredAggregator::getAlias).toList())
				.containsExactly("price");
	}

	// --- getColumns ---

	@Test
	public void getColumns_skipsEmptyAggregatorColumnName() {
		// Aggregator.empty() carries an `EmptyAggregation`; it never reads from the table — its column name
		// (defaulted to its name "empty") is a placeholder, not a real column. `getColumns` is consumed by
		// downstream JOIN-pruning to decide which tables a query touches, so feeding it the placeholder would
		// either spuriously pull in a join (if the supplier blindly trusts it) or trigger a strict-mode
		// rejection in `PrunedJoinsJooqTableSupplier.computeNeededAliases`. Empty aggregators must be skipped.
		FilteredAggregator aggEmpty = FilteredAggregator.builder().aggregator(Aggregator.empty()).build();

		TableQueryV4 v4 = TableQueryV4.builder()
				.groupByToAggregators(ImmutableSetMultimap.of(gbCountry, aggSum, gbCountry, aggEmpty))
				.build();

		Set<String> columns = TableQueryV4.getColumns(v4);

		// `country` from the groupBy and `price` from the real aggregator must be present; the empty
		// aggregator's "empty" placeholder column name must not.
		Assertions.assertThat(columns).contains("country", "price").doesNotContain("empty");
	}
}

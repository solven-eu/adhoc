/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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

import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.value.AndMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestTableQueryV2 {
	Aggregator sumK1 = Aggregator.sum("k1");
	TableQuery groupByA = TableQuery.builder().groupBy(GroupByColumns.named("a")).aggregator(sumK1).build();
	TableQuery filterA1 = TableQuery.builder()
			.groupBy(GroupByColumns.named("a"))
			.aggregator(sumK1)
			.filter(AndFilter.and(Map.of("a", "a1")))
			.build();
	TableQuery filterA1B1 = TableQuery.builder()
			.groupBy(GroupByColumns.named("a"))
			.aggregator(sumK1)
			.filter(AndFilter.and(ImmutableMap.of("a", "a1", "b", "b1")))
			.build();
	TableQuery filterA12 = TableQuery.builder()
			.groupBy(GroupByColumns.named("a"))
			.aggregator(sumK1)
			.filter(AndFilter.and(ImmutableMap.of("a", ImmutableSet.of("a1", "a2"))))
			.build();

	TableQuery filterAPrefix = TableQuery.builder()
			.groupBy(GroupByColumns.named("a"))
			.aggregator(sumK1)
			.filter(AndFilter.and(ImmutableMap.of("a", LikeMatcher.matching("a%"))))
			.build();

	TableQuery filterAPrefixNotAzerty = TableQuery.builder()
			.groupBy(GroupByColumns.named("a"))
			.aggregator(sumK1)
			.filter(AndFilter.and(ImmutableMap.of("a",
					AndMatcher.and(LikeMatcher.matching("a%"), NotMatcher.not(EqualsMatcher.matchEq("azerty"))))))
			.build();

	Aggregator sumK2 = Aggregator.sum("k2");
	TableQuery groupByA_K2 = TableQuery.builder().groupBy(GroupByColumns.named("a")).aggregator(sumK2).build();

	@Test
	public void testGrandTotalAndFilterA1() {
		Set<TableQueryV2> v2 = TableQueryV2.fromV1(ImmutableSet.of(groupByA, filterA1));
		Assertions.assertThat(v2)
				.hasSize(1)
				.contains(TableQueryV2.builder()
						.groupBy(GroupByColumns.named("a"))
						.aggregator(
								FilteredAggregator.builder().aggregator(sumK1).filter(AndFilter.and(Map.of())).build())
						.aggregator(FilteredAggregator.builder()
								.aggregator(sumK1)
								.filter(AndFilter.and(Map.of("a", "a1")))
								.index(1)
								.build())
						.build());
	}

	@Test
	public void testFilterA1FilterA1B1() {
		Set<TableQueryV2> v2 = TableQueryV2.fromV1(ImmutableSet.of(filterA1, filterA1B1));
		Assertions.assertThat(v2)
				.hasSize(1)
				.contains(TableQueryV2.builder()
						.groupBy(GroupByColumns.named("a"))
						.aggregator(
								FilteredAggregator.builder().aggregator(sumK1).filter(AndFilter.and(Map.of())).build())
						.aggregator(FilteredAggregator.builder()
								.aggregator(sumK1)
								.filter(AndFilter.and(Map.of("b", "b1")))
								.index(1)
								.build())
						.filter(AndFilter.and(Map.of("a", "a1")))
						.build());
	}

	// IN is not AND
	@Test
	public void testFilterA1FilterA12() {
		Set<TableQueryV2> v2 = TableQueryV2.fromV1(ImmutableSet.of(filterA1, filterA12));
		Assertions.assertThat(v2)
				.hasSize(1)
				.contains(TableQueryV2.builder()
						.groupBy(GroupByColumns.named("a"))
						.aggregator(FilteredAggregator.builder()
								.aggregator(sumK1)
								.filter(AndFilter.and(Map.of("a", "a1")))
								.build())
						.aggregator(FilteredAggregator.builder()
								.aggregator(sumK1)
								.filter(AndFilter.and(Map.of("a", Set.of("a1", "a2"))))
								.index(1)
								.build())
						// .filter(AndFilter.and(Map.of("a", "a1")))
						.build());
	}

	@Test
	public void testAndMatcher() {
		Set<TableQueryV2> v2 = TableQueryV2.fromV1(ImmutableSet.of(filterAPrefix, filterAPrefixNotAzerty));
		Assertions.assertThat(v2).singleElement().satisfies(q -> {
			Assertions.assertThat(q.getGroupBy()).isEqualTo(GroupByColumns.named("a"));
			Assertions.assertThat(q.getFilter()).isEqualTo(ColumnFilter.matchLike("a", "a%"));
			Assertions.assertThat(q.getAggregators())
					.hasSize(2)
					.contains(FilteredAggregator.builder().aggregator(sumK1).filter(ISliceFilter.MATCH_ALL).build())
					.contains(FilteredAggregator.builder()
							.aggregator(sumK1)
							.filter(AndFilter.and(Map.of("a", NotMatcher.not(EqualsMatcher.matchEq("azerty")))))
							.index(1)
							.build());
		});
	}

	@Test
	public void testDifferentAggregators() {
		Set<TableQueryV2> v2 = TableQueryV2.fromV1(ImmutableSet.of(groupByA, groupByA_K2));
		Assertions.assertThat(v2)
				.hasSize(1)
				.contains(TableQueryV2.builder()
						.groupBy(GroupByColumns.named("a"))
						.aggregator(
								FilteredAggregator.builder().aggregator(sumK1).filter(AndFilter.and(Map.of())).build())
						.aggregator(
								FilteredAggregator.builder().aggregator(sumK2).filter(AndFilter.and(Map.of())).build())
						.build());
	}

	@Test
	public void testNoMeasure() {
		Set<TableQueryV2> v2 = TableQueryV2.fromV1(ImmutableSet
				.of(TableQuery.builder().filter(AndFilter.and(ImmutableMap.of("k1", "v1", "k2", "v2"))).build()));
		Assertions.assertThat(v2)
				.hasSize(1)
				.contains(TableQueryV2.builder()
						.groupBy(IAdhocGroupBy.GRAND_TOTAL)
						.filter(AndFilter.and(ImmutableMap.of("k1", "v1", "k2", "v2")))
						.aggregator(FilteredAggregator.builder()
								.aggregator(Aggregator.empty())
								.filter(ISliceFilter.MATCH_ALL)
								.build())
						.build());
	}

}

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
package eu.solven.adhoc.table.sql;

import org.assertj.core.api.Assertions;
import org.jooq.Condition;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MinAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.RankAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.NotFilter;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;
import eu.solven.adhoc.query.filter.value.StringMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV2;

public class TestJooqTableQueryFactory_DuckDb {
	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	JooqTableQueryFactory queryFactory = JooqTableQueryFactory.builder()
			.table(DSL.table(DSL.name("someTableName")))
			.dslContext(DSL.using(SQLDialect.DUCKDB))
			.build();

	@Test
	public void testBuilder() {
		Assertions.assertThat(queryFactory.getCapabilities().isAbleToGroupByAll()).isTrue();
		Assertions.assertThat(queryFactory.getCapabilities().isAbleToFilterAggregates()).isTrue();
	}

	@Test
	public void testToCondition_ColumnEquals() {
		Condition condition = queryFactory.toConditionSplitLeftover(ColumnFilter.matchEq("k1", "v1")).getCondition();

		Assertions.assertThat(condition.toString()).isEqualTo("""
				"k1" = 'v1'""");
	}

	@Test
	public void testToCondition_AndColumnsEquals() {
		// ImmutableMap for ordering, as we later check the .toString
		JooqTableQueryFactory.ConditionWithFilter condition =
				queryFactory.toConditionSplitLeftover(AndFilter.and(ImmutableMap.of("k1", "v1", "k2", "v2")));

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getCondition().toString()).isEqualTo("""
				(
				  "k1" = 'v1'
				  and "k2" = 'v2'
				)""");
	}

	@Test
	public void testToCondition_OrColumnsEquals() {
		ISliceFilter filter =
				FilterBuilder.or(ColumnFilter.matchEq("k1", "v1"), ColumnFilter.matchEq("k2", "v2")).combine();
		JooqTableQueryFactory.ConditionWithFilter condition = queryFactory.toConditionSplitLeftover(filter);

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getCondition().toString()).isEqualTo("""
				(
				  "k1" = 'v1'
				  or "k2" = 'v2'
				)""");
	}

	@Test
	public void testToCondition_NotFilter() {
		ISliceFilter filter =
				FilterBuilder.or(ColumnFilter.matchEq("k1", "v1"), ColumnFilter.matchEq("k2", "v2")).combine().negate();
		JooqTableQueryFactory.ConditionWithFilter condition = queryFactory.toConditionSplitLeftover(filter);

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getCondition().toString()).isEqualTo("""
				not (
				  (
				    "k1" is not null
				    and "k1" = 'v1'
				  )
				  or (
				    "k2" is not null
				    and "k2" = 'v2'
				  )
				)""");
	}

	@Test
	public void testToCondition_NotMatcher() {
		ISliceFilter filter =
				FilterBuilder.and(ColumnFilter.notEq("k1", "v1"), ColumnFilter.notEq("k2", "v2")).combine();
		JooqTableQueryFactory.ConditionWithFilter condition = queryFactory.toConditionSplitLeftover(filter);

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getCondition().toString()).isEqualTo("""
				(
				  not (
				    "k1" is not null
				    and "k1" = 'v1'
				  )
				  and not (
				    "k2" is not null
				    and "k2" = 'v2'
				  )
				)""");
	}

	@Test
	public void testGrandTotal() {
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory
				.prepareQuery(TableQuery.builder().aggregator(Aggregator.builder().name("k").build()).build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("k") "k" from "someTableName" group by ALL""");
	}

	@Test
	public void testMeasureNameWithDot() {
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(
				TableQuery.builder().aggregator(Aggregator.builder().name("k.USD").columnName("t.k").build()).build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("t"."k") "k.USD" from "someTableName" group by ALL""");
	}

	@Test
	public void testMeasureNameWithDot_quoted() {
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(TableQuery.builder()
				.aggregator(Aggregator.builder().name("k.USD").columnName("\"t.k\"").build())
				.build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("t.k") "k.USD" from "someTableName" group by ALL""");
	}

	@Test
	public void testColumnWithAt() {
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(TableQuery.builder()
				.aggregator(Aggregator.builder().name("k").build())
				.groupBy(GroupByColumns.named("a@b@c"))
				.build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("k") "k", "a@b@c" from "someTableName" group by ALL""");
	}

	@Test
	public void testColumnWithSpace() {
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(TableQuery.builder()
				.aggregator(Aggregator.builder().name("k").build())
				.groupBy(GroupByColumns.named("pre post"))
				.build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("k") "k", "pre post" from "someTableName" group by ALL""");
	}

	@Test
	public void testCountAsterisk() {
		IJooqTableQueryFactory.QueryWithLeftover condition =
				queryFactory.prepareQuery(TableQuery.builder().aggregator(Aggregator.countAsterisk()).build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select count(*) "count(*)" from "someTableName" group by ALL""");
	}

	@Test
	public void testEmptyAggregation() {
		IJooqTableQueryFactory.QueryWithLeftover condition =
				queryFactory.prepareQuery(TableQuery.builder().aggregator(Aggregator.empty()).build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select count(1) from "someTableName" group by ALL""");
	}

	@Test
	public void testFilter_custom() {
		ColumnFilter customFilter =
				ColumnFilter.builder().column("c").valueMatcher(IAdhocTestConstants.randomMatcher).build();
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory
				.prepareQuery(TableQuery.builder().aggregator(Aggregator.sum("k")).filter(customFilter).build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l).isSameAs(customFilter));
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("k") "k", "c" from "someTableName" group by ALL""");
	}

	@Test
	public void testFilter_custom_OR() {
		ColumnFilter customFilter =
				ColumnFilter.builder().column("c").valueMatcher(IAdhocTestConstants.randomMatcher).build();
		ISliceFilter orFilter = FilterBuilder.or(ColumnFilter.matchEq("d", "someD"), customFilter).combine();
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory
				.prepareQuery(TableQuery.builder().aggregator(Aggregator.sum("k")).filter(orFilter).build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l).isEqualTo(orFilter));
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("k") "k", "d", "c" from "someTableName" group by ALL""");
	}

	@Test
	public void testFilter_custom_NOT() {
		ColumnFilter customFilter =
				ColumnFilter.builder().column("c").valueMatcher(IAdhocTestConstants.randomMatcher).build();
		ISliceFilter notFilter = customFilter.negate();
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory
				.prepareQuery(TableQuery.builder().aggregator(Aggregator.sum("k")).filter(notFilter).build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l).isEqualTo(notFilter));
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("k") "k", "c" from "someTableName" group by ALL""");
	}

	// BEWARE This ensures the `NOT(OR(...))` is turned into an `AND()` for proper splitting of `postFilter`
	@Test
	public void testFilter_custom_NotOr() {
		ColumnFilter customFilter =
				ColumnFilter.builder().column("c").valueMatcher(IAdhocTestConstants.randomMatcher).build();
		ISliceFilter notFilter = NotFilter.builder()
				.negated(FilterBuilder.or(ColumnFilter.matchEq("d", "someD"), customFilter).combine())
				.build();
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory
				.prepareQuery(TableQuery.builder().aggregator(Aggregator.sum("k")).filter(notFilter).build());

		// custom part is handled as leftover
		Assertions.assertThat(condition.getLeftover())
				.satisfies(l -> Assertions.assertThat(l).isEqualTo(customFilter.negate()));
		// native part is managed by SQL: requesting `c` as groupBy to enable late filtering
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED))
				.isEqualTo(
						"""
								select sum("k") "k", "c" from "someTableName" where not ("d" is not null and "d" = 'someD') group by ALL""");
	}

	@Test
	public void testFilteredAggregator() {
		ISliceFilter customFilter = ColumnFilter.matchEq("c", "c1");
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(TableQueryV2.builder()
				.aggregator(FilteredAggregator.builder().aggregator(Aggregator.sum("k")).filter(customFilter).build())
				.build());

		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("k") filter (where "c" = 'c1') "k" from "someTableName" group by ALL""");
	}

	@Test
	public void testFilteredAggregator_rank() {
		ISliceFilter customFilter = ColumnFilter.matchEq("c", "c1");
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(TableQueryV2.builder()
				.aggregator(FilteredAggregator.builder()
						.aggregator(Aggregator.builder()
								.name("rankM")
								.columnName("rankC")
								.aggregationKey(RankAggregation.KEY)
								.aggregationOption(RankAggregation.P_RANK, 2)
								.build())
						.filter(customFilter)
						.build())
				.build());

		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED))
				.isEqualTo(
						"""
								select arg_max("rankC", "rankC", 2) filter (where "c" = 'c1') "rankM" from "someTableName" group by ALL""");
	}

	@Test
	public void testMinMax() {
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(TableQuery.builder()
				.aggregator(Aggregator.builder().name("kMin").aggregationKey(MinAggregation.KEY).build())
				.aggregator(Aggregator.builder().name("kMax").aggregationKey(MaxAggregation.KEY).build())
				.build());

		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select min("kMin") "kMin", max("kMax") "kMax" from "someTableName" group by ALL""");
	}

	@Test
	public void testRank() {
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(TableQuery.builder()
				.aggregator(Aggregator.builder()
						.name("kRank")
						.aggregationKey(RankAggregation.KEY)
						.aggregationOption(RankAggregation.P_RANK, 3)
						.aggregationOption(RankAggregation.P_ORDER, "ASC")
						.build())
				.build());

		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select arg_min("kRank", "kRank", 3) "kRank" from "someTableName" group by ALL""");
	}

	@Test
	public void testFilter_StringMatcher() {
		ColumnFilter customFilter =
				ColumnFilter.builder().column("c").matching(StringMatcher.hasToString("c1")).build();
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(TableQueryV2.builder()
				.aggregator(FilteredAggregator.builder().aggregator(Aggregator.sum("k")).filter(customFilter).build())
				.build());

		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("k") filter (where cast("c" as varchar) = 'c1') "k" from "someTableName" group by ALL""");
	}

	@Test
	public void testFilter_OrStringMatcher() {
		ColumnFilter customFilter = ColumnFilter.builder()
				.column("c")
				.matching(OrMatcher.or(StringMatcher.hasToString("c1"), StringMatcher.hasToString("c2")))
				.build();
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(TableQueryV2.builder()
				.aggregator(FilteredAggregator.builder().aggregator(Aggregator.sum("k")).filter(customFilter).build())
				.build());

		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED))
				.isEqualTo(
						"""
								select sum("k") filter (where (cast("c" as varchar) = 'c1' or cast("c" as varchar) = 'c2')) "k" from "someTableName" group by ALL""");
	}

	@Test
	public void testFilter_NotStringMatcher() {
		ColumnFilter customFilter =
				ColumnFilter.builder().column("c").matching(NotMatcher.not(StringMatcher.hasToString("c1"))).build();
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(TableQueryV2.builder()
				.aggregator(FilteredAggregator.builder().aggregator(Aggregator.sum("k")).filter(customFilter).build())
				.build());

		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED))
				.isEqualTo(
						"""
								select sum("k") filter (where not ("c" is not null and cast("c" as varchar) = 'c1')) "k" from "someTableName" group by ALL""");
	}

	@Test
	public void testFilteredAggregator_custom() {
		ISliceFilter nativeFilter = ColumnFilter.matchEq("c", "c1");
		ColumnFilter customFilter =
				ColumnFilter.builder().column("c").valueMatcher(IAdhocTestConstants.randomMatcher).build();
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(TableQueryV2.builder()
				.aggregator(FilteredAggregator.builder()
						.aggregator(Aggregator.sum("k"))
						.filter(nativeFilter)
						.index(0)
						.build())
				.aggregator(FilteredAggregator.builder()
						.aggregator(Aggregator.sum("k"))
						.filter(customFilter)
						.index(1)
						.build())
				.build());

		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED))
				.isEqualTo(
						"""
								select sum("k") filter (where "c" = 'c1') "k", sum("k") "k_1", "c" from "someTableName" group by ALL""");
	}

}

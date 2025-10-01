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
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV2;

public class TestJooqTableQueryFactory_Postgres {
	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	JooqTableQueryFactory queryFactory = JooqTableQueryFactory.builder()
			.table(DSL.table(DSL.name("someTableName")))
			.dslContext(DSL.using(SQLDialect.POSTGRES))
			.build();

	@Test
	public void testToCondition_ColumnEquals() {
		Condition condition = queryFactory.toCondition(ColumnFilter.equalTo("k1", "v1")).getCondition();

		Assertions.assertThat(condition.toString()).isEqualTo("""
				"k1" = 'v1'
				""".trim());
	}

	@Test
	public void testToCondition_AndColumnsEquals() {
		// ImmutableMap for ordering, as we later check the .toString
		JooqTableQueryFactory.ConditionWithFilter condition =
				queryFactory.toCondition(AndFilter.and(ImmutableMap.of("k1", "v1", "k2", "v2")));

		Assertions.assertThat(condition.getPostFilter()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getCondition().toString()).isEqualTo("""
				(
				  "k1" = 'v1'
				  and "k2" = 'v2'
				)""");
	}

	@Test
	public void testToCondition_OrColumnsEquals() {
		ISliceFilter filter =
				FilterBuilder.or(ColumnFilter.equalTo("k1", "v1"), ColumnFilter.equalTo("k2", "v2")).optimize();
		JooqTableQueryFactory.ConditionWithFilter condition = queryFactory.toCondition(filter);

		Assertions.assertThat(condition.getPostFilter()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getCondition().toString()).isEqualTo("""
				(
				  "k1" = 'v1'
				  or "k2" = 'v2'
				)""");
	}

	@Test
	public void testToCondition_Not() {
		ISliceFilter filter = NotFilter
				.not(FilterBuilder.or(ColumnFilter.equalTo("k1", "v1"), ColumnFilter.equalTo("k2", "v2")).optimize());
		JooqTableQueryFactory.ConditionWithFilter condition = queryFactory.toCondition(filter);

		Assertions.assertThat(condition.getPostFilter()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getCondition().toString()).isEqualTo("""
				(
				  not ("k1" = 'v1')
				  and not ("k2" = 'v2')
				)""");
	}

	@Test
	public void testGrandTotal() {
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory
				.prepareQuery(TableQuery.builder().aggregator(Aggregator.builder().name("k").build()).build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("k") as "k" from "someTableName" group by ()""");
	}

	@Test
	public void testMeasureNameWithDot() {
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(
				TableQuery.builder().aggregator(Aggregator.builder().name("k.USD").columnName("t.k").build()).build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("t"."k") as "k.USD" from "someTableName" group by ()""");
	}

	@Test
	public void testColumnWithAt() {
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(TableQuery.builder()
				.aggregator(Aggregator.builder().name("k").build())
				.groupBy(GroupByColumns.named("a@b@c"))
				.build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("k") as "k", "a@b@c" from "someTableName" group by "a@b@c"
				""".trim());
	}

	@Test
	public void testColumnWithSpace() {
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(TableQuery.builder()
				.aggregator(Aggregator.builder().name("k").build())
				.groupBy(GroupByColumns.named("pre post"))
				.build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("k") as "k", "pre post" from "someTableName" group by "pre post"
				""".trim());
	}

	@Test
	public void testCountAsterisk() {
		IJooqTableQueryFactory.QueryWithLeftover condition =
				queryFactory.prepareQuery(TableQuery.builder().aggregator(Aggregator.countAsterisk()).build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select count(*) as "count(*)" from "someTableName" group by ()
				""".trim());
	}

	@Test
	public void testEmptyAggregation() {
		IJooqTableQueryFactory.QueryWithLeftover condition =
				queryFactory.prepareQuery(TableQuery.builder().aggregator(Aggregator.empty()).build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select count(1) from "someTableName" group by ()
				""".trim());
	}

	@Test
	public void testFilter_custom() {
		ColumnFilter customFilter =
				ColumnFilter.builder().column("c").valueMatcher(IAdhocTestConstants.randomMatcher).build();
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory
				.prepareQuery(TableQuery.builder().aggregator(Aggregator.sum("k")).filter(customFilter).build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l).isSameAs(customFilter));
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("k") as "k", "c" from "someTableName" group by "c"
				""".trim());
	}

	@Test
	public void testFilter_custom_OR() {
		ColumnFilter customFilter =
				ColumnFilter.builder().column("c").valueMatcher(IAdhocTestConstants.randomMatcher).build();
		ISliceFilter orFilter = FilterBuilder.or(ColumnFilter.equalTo("d", "someD"), customFilter).optimize();
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory
				.prepareQuery(TableQuery.builder().aggregator(Aggregator.sum("k")).filter(orFilter).build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l).isEqualTo(orFilter));
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("k") as "k", "c", "d" from "someTableName" group by "c", "d"
				""".trim());
	}

	@Test
	public void testFilter_custom_NOT() {
		ColumnFilter customFilter =
				ColumnFilter.builder().column("c").valueMatcher(IAdhocTestConstants.randomMatcher).build();
		ISliceFilter orFilter =
				NotFilter.not(FilterBuilder.or(ColumnFilter.equalTo("d", "someD"), customFilter).optimize());
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory
				.prepareQuery(TableQuery.builder().aggregator(Aggregator.sum("k")).filter(orFilter).build());

		Assertions.assertThat(condition.getLeftover())
				.satisfies(l -> Assertions.assertThat(l).isEqualTo(NotFilter.not(customFilter)));
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("k") as "k", "c" from "someTableName" where not ("d" = 'someD') group by "c"
								""".trim());
	}

	@Test
	public void testFilteredAggregator() {
		ISliceFilter customFilter = ColumnFilter.equalTo("c", "c1");
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(TableQueryV2.builder()
				.aggregator(FilteredAggregator.builder().aggregator(Aggregator.sum("k")).filter(customFilter).build())
				.build());

		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("k") filter (where "c" = 'c1') as "k" from "someTableName" group by ()""");
	}

	@Test
	public void testFilteredAggregator_rank() {
		ISliceFilter customFilter = ColumnFilter.equalTo("c", "c1");
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
								select arg_max("rankc", "rankc", 2) filter (where "c" = 'c1') as "rankM" from "someTableName" group by ()""");
	}

	@Test
	public void testMinMax() {
		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(TableQuery.builder()
				.aggregator(Aggregator.builder().name("kMin").aggregationKey(MinAggregation.KEY).build())
				.aggregator(Aggregator.builder().name("kMax").aggregationKey(MaxAggregation.KEY).build())
				.build());

		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select min("kmin") as "kMin", max("kmax") as "kMax" from "someTableName" group by ()""");
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
				select arg_min("krank", "krank", 3) as "kRank" from "someTableName" group by ()""");
	}

}

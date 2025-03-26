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
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.measure.StandardOperatorsFactory;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.table.TableQuery;

public class TestAdhocJooqTableQueryFactory {
	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	AdhocJooqTableQueryFactory streamOpener = new AdhocJooqTableQueryFactory(new StandardOperatorsFactory(),
			DSL.table(DSL.name("someTableName")),
			DSL.using(SQLDialect.DUCKDB));

	@Test
	public void testToCondition_ColumnEquals() {
		Condition condition = streamOpener.toCondition(ColumnFilter.isEqualTo("k1", "v1"));

		Assertions.assertThat(condition.toString()).isEqualTo("""
				"k1" = 'v1'
				""".trim());
	}

	@Test
	public void testToCondition_AndColumnsEquals() {
		// ImmutableMap for ordering, as we later check the .toString
		Condition condition = streamOpener.toCondition(AndFilter.and(ImmutableMap.of("k1", "v1", "k2", "v2")));

		Assertions.assertThat(condition.toString()).isEqualTo("""
				(
				  "k1" = 'v1'
				  and "k2" = 'v2'
				)
				""".trim());
	}

	@Test
	public void testToCondition_OrColumnsEquals() {
		Condition condition = streamOpener
				.toCondition(OrFilter.or(ColumnFilter.isEqualTo("k1", "v1"), ColumnFilter.isEqualTo("k2", "v2")));

		Assertions.assertThat(condition.toString()).isEqualTo("""
				(
				  "k1" = 'v1'
				  or "k2" = 'v2'
				)
				""".trim());
	}

	@Test
	public void testGrandTotal() {
		ResultQuery<Record> condition = streamOpener
				.prepareQuery(TableQuery.builder().aggregator(Aggregator.builder().name("k").build()).build());

		Assertions.assertThat(condition.getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("k") "k" from "someTableName" where "k" is not null group by ()
				""".trim());
	}

	@Test
	public void testMeasureNameWithDot() {
		ResultQuery<Record> condition = streamOpener.prepareQuery(
				TableQuery.builder().aggregator(Aggregator.builder().name("k.USD").columnName("t.k").build()).build());

		Assertions.assertThat(condition.getSQL(ParamType.INLINED)).isEqualTo("""
				select sum("t"."k") "k.USD" from "someTableName" where "t"."k" is not null group by ()
				""".trim());
	}

	@Test
	public void testCountAsterisk() {
		ResultQuery<Record> condition =
				streamOpener.prepareQuery(TableQuery.builder().aggregator(Aggregator.countAsterisk()).build());

		Assertions.assertThat(condition.getSQL(ParamType.INLINED)).isEqualTo("""
				select count(*) "count(*)" from "someTableName" group by ()
				""".trim());
	}

	@Test
	public void testEmptyAggregation() {
		ResultQuery<Record> condition =
				streamOpener.prepareQuery(TableQuery.builder().aggregator(Aggregator.empty()).build());

		Assertions.assertThat(condition.getSQL(ParamType.INLINED)).isEqualTo("""
				select 1 from "someTableName" group by ()
				""".trim());
	}
}

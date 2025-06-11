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
package eu.solven.adhoc.table.sql.duckdb;

import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.sql.DSLSupplier;
import eu.solven.adhoc.table.sql.IJooqTableQueryFactory;
import eu.solven.adhoc.table.sql.JooqTableQueryFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestDuckDbHelper {
	@Test
	public void testFilterExpr_like() {
		Assertions.assertThat(DuckDbHelper.toFilterExpression(LikeMatcher.matching("%abc"))).isEqualTo("LIKE '%abc'");
	}

	@Test
	public void testFilterExpr_notLike() {
		Assertions.assertThat(DuckDbHelper.toFilterExpression(NotMatcher.not(LikeMatcher.matching("%abc"))))
				.isEqualTo("NOT LIKE '%abc'");
	}

	@Test
	public void testFilterExpr_matchAll() {
		Assertions.assertThat(DuckDbHelper.toFilterExpression(IValueMatcher.MATCH_ALL)).isEqualTo("1 = 1");
	}

	@Test
	public void testFilterExpr_matchNone() {
		Assertions.assertThat(DuckDbHelper.toFilterExpression(IValueMatcher.MATCH_NONE)).isEqualTo("1 = 0");
	}

	@Test
	public void testAppendGetCoordinatesMeasures_qualified() {
		TableQuery.TableQueryBuilder tableQueryBuilder = TableQuery.builder();

		DuckDbHelper.appendGetCoordinatesMeasures(123, "table.column", IValueMatcher.MATCH_ALL, 7, tableQueryBuilder);

		JooqTableQueryFactory queryFactory = JooqTableQueryFactory.builder()
				.table(DSL.table("someTable"))
				.dslContext(DSL.using(SQLDialect.DUCKDB))
				.build();

		IJooqTableQueryFactory.QueryWithLeftover condition = queryFactory.prepareQuery(tableQueryBuilder.build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED))
				.isEqualTo(
						"""
								select approx_count_distinct("table"."column") "approx_count_distinct_7", approx_top_k("table"."column", 123) "approx_top_k_7" from someTable group by ALL""");
	}

	@Test
	public void testAppendGetCoordinatesMeasures_qualified_quoted() {
		TableQuery.TableQueryBuilder tableQueryBuilder = TableQuery.builder();

		DuckDbHelper.appendGetCoordinatesMeasures(123,
				"\"table\".\"column\"",
				IValueMatcher.MATCH_ALL,
				7,
				tableQueryBuilder);

		JooqTableQueryFactory queryFactory = JooqTableQueryFactory.builder()
				.table(DSL.table("someTable"))
				.dslContext(DSL.using(SQLDialect.DUCKDB))
				.build();

		IJooqTableQueryFactory.QueryWithLeftover queryWithLeftover =
				queryFactory.prepareQuery(tableQueryBuilder.build());

		Assertions.assertThat(queryWithLeftover.getLeftover())
				.satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(queryWithLeftover.getQuery().getSQL(ParamType.INLINED))
				.isEqualTo(
						"""
								select approx_count_distinct("table"."column") "approx_count_distinct_7", approx_top_k("table"."column", 123) "approx_top_k_7" from someTable group by ALL""");
	}

	@Test
	public void testAppendGetCoordinatesMeasures_withSpace() {
		TableQuery.TableQueryBuilder tableQueryBuilder = TableQuery.builder();

		DuckDbHelper.appendGetCoordinatesMeasures(123, "pre post", IValueMatcher.MATCH_ALL, 7, tableQueryBuilder);

		JooqTableQueryFactory queryFactory = JooqTableQueryFactory.builder()
				.table(DSL.table("someTable"))
				.dslContext(DSL.using(SQLDialect.DUCKDB))
				.build();

		IJooqTableQueryFactory.QueryWithLeftover queryWithLeftover =
				queryFactory.prepareQuery(tableQueryBuilder.build());

		Assertions.assertThat(queryWithLeftover.getLeftover())
				.satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(queryWithLeftover.getQuery().getSQL(ParamType.INLINED))
				.isEqualTo(
						"""
								select approx_count_distinct("pre post") "approx_count_distinct_7", approx_top_k("pre post", 123) "approx_top_k_7" from someTable group by ALL""");
	}

	@Disabled
	@Test
	public void testMakeConnections_withPool() {
		// https://github.com/brettwooldridge/HikariCP?tab=readme-ov-file#rocket-initialization
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(DuckDbHelper.getInMemoryJdbcUrl());

		HikariDataSource ds = new HikariDataSource(config);

		DSLSupplier supplier = DSLSupplier.fromDatasource(ds, SQLDialect.DUCKDB);

		IntStream.range(0, 16 * 1024).forEach(i -> {
			if (Integer.bitCount(i + 1) == 1) {
				log.info("testMakeConnections_withPool #{}", i);
			}
			supplier.getDSLContext();
		});
	}
}

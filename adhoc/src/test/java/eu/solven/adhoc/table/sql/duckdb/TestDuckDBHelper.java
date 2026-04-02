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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.filter.value.LikeMatcher;
import eu.solven.adhoc.filter.value.NotMatcher;
import eu.solven.adhoc.filter.value.StringMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.sql.AdhocJooqHelper;
import eu.solven.adhoc.table.sql.IDSLSupplier;
import eu.solven.adhoc.table.sql.JooqTableQueryFactory;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.QueryWithLeftover;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestDuckDBHelper {
	static {
		AdhocJooqHelper.disableBanners();
	}

	@Test
	public void testFilterExpr_like() {
		Assertions.assertThat(DuckDBHelper.toFilterExpression(LikeMatcher.matching("%abc"))).isEqualTo("LIKE '%abc'");
	}

	@Test
	public void testFilterExpr_notLike() {
		Assertions.assertThat(DuckDBHelper.toFilterExpression(NotMatcher.not(LikeMatcher.matching("%abc"))))
				.isEqualTo("NOT LIKE '%abc'");
	}

	@Test
	public void testFilterExpr_matchAll() {
		Assertions.assertThat(DuckDBHelper.toFilterExpression(IValueMatcher.MATCH_ALL)).isEqualTo("1 = 1");
	}

	@Test
	public void testFilterExpr_matchNone() {
		Assertions.assertThat(DuckDBHelper.toFilterExpression(IValueMatcher.MATCH_NONE)).isEqualTo("1 = 0");
	}

	@Test
	public void testAppendGetCoordinatesMeasures_qualified() {
		TableQuery.TableQueryBuilder tableQueryBuilder = TableQuery.builder();

		DuckDBHelper.appendGetCoordinatesMeasures(123, "table.column", IValueMatcher.MATCH_ALL, 7, tableQueryBuilder);

		JooqTableQueryFactory queryFactory = JooqTableQueryFactory.builder()
				.table(DSL.table("someTable"))
				.dslContext(DSL.using(SQLDialect.DUCKDB))
				.build();

		QueryWithLeftover condition = queryFactory.prepareQuery(tableQueryBuilder.build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getQuery().getSQL(ParamType.INLINED))
				.isEqualTo(
						"""
								select approx_count_distinct("table"."column") "approx_count_distinct_7", approx_top_k("table"."column", 123) "approx_top_k_7" from someTable group by ALL""");
	}

	@Test
	public void testAppendGetCoordinatesMeasures_qualified_quoted() {
		TableQuery.TableQueryBuilder tableQueryBuilder = TableQuery.builder();

		DuckDBHelper.appendGetCoordinatesMeasures(123,
				"\"table\".\"column\"",
				IValueMatcher.MATCH_ALL,
				7,
				tableQueryBuilder);

		JooqTableQueryFactory queryFactory = JooqTableQueryFactory.builder()
				.table(DSL.table("someTable"))
				.dslContext(DSL.using(SQLDialect.DUCKDB))
				.build();

		QueryWithLeftover queryWithLeftover = queryFactory.prepareQuery(tableQueryBuilder.build());

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

		DuckDBHelper.appendGetCoordinatesMeasures(123, "pre post", IValueMatcher.MATCH_ALL, 7, tableQueryBuilder);

		JooqTableQueryFactory queryFactory = JooqTableQueryFactory.builder()
				.table(DSL.table("someTable"))
				.dslContext(DSL.using(SQLDialect.DUCKDB))
				.build();

		QueryWithLeftover queryWithLeftover = queryFactory.prepareQuery(tableQueryBuilder.build());

		Assertions.assertThat(queryWithLeftover.getLeftover())
				.satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(queryWithLeftover.getQuery().getSQL(ParamType.INLINED))
				.isEqualTo(
						"""
								select approx_count_distinct("pre post") "approx_count_distinct_7", approx_top_k("pre post", 123) "approx_top_k_7" from someTable group by ALL""");
	}

	@Test
	public void test_prepareQuery_misOrderedColumns() {
		TableQuery.TableQueryBuilder tableQueryBuilder = TableQuery.builder();

		JooqTableQueryFactory queryFactory = JooqTableQueryFactory.builder()
				.table(DSL.table("someTable"))
				.dslContext(DSL.using(SQLDialect.DUCKDB))
				.build();

		QueryWithLeftover queryWithLeftover =
				queryFactory.prepareQuery(tableQueryBuilder.groupBy(GroupByColumns.named("b", "a")).build());

		Assertions.assertThat(queryWithLeftover.getLeftover())
				.satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(queryWithLeftover.getQuery().getSQL(ParamType.INLINED)).isEqualTo("""
				select "b", "a" from someTable group by ALL""");
	}

	@Test
	public void testMakeConnections_withPool() {
		// https://github.com/brettwooldridge/HikariCP?tab=readme-ov-file#rocket-initialization
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(DuckDBHelper.getInMemoryJdbcUrl());
		config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(5));
		config.setAutoCommit(false);

		HikariDataSource ds = new HikariDataSource(config);

		IDSLSupplier supplier = IDSLSupplier.fromDatasource(ds, SQLDialect.DUCKDB);

		IntStream.rangeClosed(0, 16 * 1024).forEach(i -> {
			if (Integer.bitCount(i) == 1) {
				log.info("testMakeConnections_withPool #{}", i);
			}
			supplier.getDSLContext();
		});
	}

	@Test
	public void testGetCoordinates_empty() {
		IDSLSupplier dslSupplier = DuckDBHelper.inMemoryDSLSupplier();

		dslSupplier.getDSLContext().createTable("someTable").column("someColumn", SQLDataType.VARCHAR).execute();

		JooqTableWrapperParameters tableParameters =
				DuckDBHelper.parametersBuilder(dslSupplier).tableName("someTable").build();
		CoordinatesSample sample = DuckDBHelper.getCoordinates(
				JooqTableWrapper.builder().name("someTable").tableParameters(tableParameters).build(),
				"someColumn",
				IValueMatcher.MATCH_ALL,
				7);

		Assertions.assertThat(sample.getEstimatedCardinality()).isEqualTo(0);
		Assertions.assertThat(sample.getCoordinates()).isEmpty();
	}

	@Test
	public void testGetCoordinates_unknown_matchAll() {
		IDSLSupplier dslSupplier = DuckDBHelper.inMemoryDSLSupplier();

		dslSupplier.getDSLContext().createTable("someTable").column("someColumn", SQLDataType.VARCHAR).execute();

		JooqTableWrapperParameters tableParameters =
				DuckDBHelper.parametersBuilder(dslSupplier).tableName("someTable").build();
		JooqTableWrapper table = JooqTableWrapper.builder().name("someTable").tableParameters(tableParameters).build();
		Assertions
				.assertThatThrownBy(
						() -> DuckDBHelper.getCoordinates(table, "unknownColumn", IValueMatcher.MATCH_ALL, 7))
				.isInstanceOf(DataAccessException.class);
	}

	@Test
	public void testGetCoordinates_unknown_stringMatcher() {
		IDSLSupplier dslSupplier = DuckDBHelper.inMemoryDSLSupplier();

		dslSupplier.getDSLContext().createTable("someTable").column("someColumn", SQLDataType.VARCHAR).execute();

		JooqTableWrapperParameters tableParameters =
				DuckDBHelper.parametersBuilder(dslSupplier).tableName("someTable").build();
		JooqTableWrapper table = JooqTableWrapper.builder().name("someTable").tableParameters(tableParameters).build();
		Assertions
				.assertThatThrownBy(
						() -> DuckDBHelper.getCoordinates(table, "unknownColumn", StringMatcher.hasToString("a1"), 7))
				.isInstanceOf(DataAccessException.class);
	}

	@Test
	public void testGetCoordinates_matcherString() {
		IDSLSupplier dslSupplier = DuckDBHelper.inMemoryDSLSupplier();

		DSLContext dslContext = dslSupplier.getDSLContext();
		dslContext.createTable("someTable").column("someColumn", SQLDataType.VARCHAR).execute();
		dslContext.insertInto(DSL.table("someTable")).set(Map.of("someColumn", "a1")).execute();
		dslContext.insertInto(DSL.table("someTable")).set(Map.of("someColumn", "a2")).execute();
		dslContext.insertInto(DSL.table("someTable")).set(Map.of("someColumn", "b1")).execute();

		JooqTableWrapperParameters tableParameters =
				DuckDBHelper.parametersBuilder(dslSupplier).tableName("someTable").build();
		JooqTableWrapper table = JooqTableWrapper.builder().name("someTable").tableParameters(tableParameters).build();
		CoordinatesSample sample = DuckDBHelper.getCoordinates(table, "someColumn", StringMatcher.hasToString("a1"), 7);

		Assertions.assertThat(sample.getEstimatedCardinality()).isEqualTo(1);
		Assertions.assertThat(sample.getCoordinates()).contains("a1");
	}

	@Test
	public void testGetCoordinates_matcherLike() {
		IDSLSupplier dslSupplier = DuckDBHelper.inMemoryDSLSupplier();

		DSLContext dslContext = dslSupplier.getDSLContext();
		dslContext.createTable("someTable").column("someColumn", SQLDataType.VARCHAR).execute();
		dslContext.insertInto(DSL.table("someTable")).set(Map.of("someColumn", "a1")).execute();
		dslContext.insertInto(DSL.table("someTable")).set(Map.of("someColumn", "a2")).execute();
		dslContext.insertInto(DSL.table("someTable")).set(Map.of("someColumn", "b1")).execute();

		JooqTableWrapperParameters tableParameters =
				DuckDBHelper.parametersBuilder(dslSupplier).tableName("someTable").build();
		JooqTableWrapper table = JooqTableWrapper.builder().name("someTable").tableParameters(tableParameters).build();
		CoordinatesSample sample = DuckDBHelper.getCoordinates(table, "someColumn", LikeMatcher.matching("a%"), 7);

		Assertions.assertThat(sample.getEstimatedCardinality()).isEqualTo(2);
		Assertions.assertThat(sample.getCoordinates()).contains("a1", "a2");
	}

}

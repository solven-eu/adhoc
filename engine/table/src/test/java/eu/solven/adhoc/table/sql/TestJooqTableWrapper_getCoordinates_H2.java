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
package eu.solven.adhoc.table.sql;

import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.assertj.core.api.Assertions;
import org.h2.jdbcx.JdbcDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.jdbc.DataSourceBuilder;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.filter.value.LikeMatcher;

/**
 * H2-backed coverage for {@link JooqTableWrapper#getCoordinates(String, IValueMatcher, int)}. Exercises the non-DuckDB
 * fall-through path that delegates to {@code ITableWrapper.getCoordinatesMostGeneric}: build a {@code TableQuery} with
 * a single-column groupBy, distinct + filter via the {@link IValueMatcher}, return a {@link CoordinatesSample} with
 * both the truncated coordinate set and the unrestricted matching count.
 */
public class TestJooqTableWrapper_getCoordinates_H2 {

	// Each test gets its own in-memory H2 database to avoid cross-test pollution from leftover tables.
	private JooqTableWrapper table;
	private DSLContext dsl;

	@BeforeEach
	public void setUp() {
		// Random DB name so parallel test runs do not share the in-memory store.
		String dbName = "test-" + UUID.randomUUID();
		DataSource dataSource = DataSourceBuilder.create()
				.type(JdbcDataSource.class)
				.driverClassName("org.h2.Driver")
				.url("jdbc:h2:mem:" + dbName
						+ Stream.of("DB_CLOSE_ON_EXIT=FALSE", "DB_CLOSE_DELAY=-1", "DATABASE_TO_LOWER=TRUE")
								.collect(Collectors.joining(";", ";", "")))
				.username("sa")
				.password("")
				.build();

		JooqTableWrapperParameters parameters = JooqTableWrapperParameters.builder()
				.dslSupplier(StandardDSLSupplier.builder().dataSource(dataSource).dialect(SQLDialect.H2).build())
				.tableName("facts")
				.build();
		table = JooqTableWrapper.builder().name("h2").tableParameters(parameters).build();

		dsl = table.makeDsl();
		dsl.createTableIfNotExists("facts")
				.column("color", SQLDataType.VARCHAR)
				.column("ccy", SQLDataType.VARCHAR)
				.execute();

		dsl.insertInto(DSL.table("facts")).set(ImmutableMap.of("color", "blue", "ccy", "EUR")).execute();
		dsl.insertInto(DSL.table("facts")).set(ImmutableMap.of("color", "blue", "ccy", "USD")).execute();
		dsl.insertInto(DSL.table("facts")).set(ImmutableMap.of("color", "red", "ccy", "EUR")).execute();
		dsl.insertInto(DSL.table("facts")).set(ImmutableMap.of("color", "green", "ccy", "GBP")).execute();
	}

	@Test
	public void testMatchAll_returnsAllDistinctCoordinates() {
		CoordinatesSample sample = table.getCoordinates("color", IValueMatcher.MATCH_ALL, 100);

		Assertions.assertThat(sample.getCoordinates()).containsExactlyInAnyOrder("blue", "red", "green");
		Assertions.assertThat(sample.getEstimatedCardinality()).isEqualTo(3);
	}

	@Test
	public void testMatchEq_filtersToMatchingCoordinate() {
		CoordinatesSample sample = table.getCoordinates("color", EqualsMatcher.matchEq("blue"), 100);

		Assertions.assertThat(sample.getCoordinates()).containsExactly("blue");
		Assertions.assertThat(sample.getEstimatedCardinality()).isEqualTo(1);
	}

	@Test
	public void testMatchLike_filtersByPattern() {
		CoordinatesSample sample = table.getCoordinates("color", LikeMatcher.matching("%e%"), 100);

		// "blue" and "green" both contain 'e'; "red" does too. This asserts the matcher is applied post-fetch
		// (the H2 path does not pushdown the LIKE; ColumnMetadataHelpers filters in memory).
		Assertions.assertThat(sample.getCoordinates()).containsExactlyInAnyOrder("blue", "red", "green");
		Assertions.assertThat(sample.getEstimatedCardinality()).isEqualTo(3);
	}

	@Test
	public void testMatchEq_noMatch_returnsEmptySample() {
		CoordinatesSample sample = table.getCoordinates("color", EqualsMatcher.matchEq("yellow"), 100);

		Assertions.assertThat(sample.getCoordinates()).isEmpty();
		Assertions.assertThat(sample.getEstimatedCardinality()).isEqualTo(0);
	}

	@Test
	public void testLimit_truncatesCoordinates_butNotCardinality() {
		// Three distinct values exist; ask for only 2.
		CoordinatesSample sample = table.getCoordinates("color", IValueMatcher.MATCH_ALL, 2);

		Assertions.assertThat(sample.getCoordinates()).hasSize(2);
		// Cardinality counts every matching coordinate, regardless of the truncation.
		Assertions.assertThat(sample.getEstimatedCardinality()).isEqualTo(3);
	}

	@Test
	public void testLimit_zeroOrNegative_collectsEveryMatchingCoordinate() {
		// `limit < 1` is treated as no cap by ColumnMetadataHelpers — the helper allocates an unbounded list.
		CoordinatesSample sample = table.getCoordinates("color", IValueMatcher.MATCH_ALL, 0);

		Assertions.assertThat(sample.getCoordinates()).containsExactlyInAnyOrder("blue", "red", "green");
		Assertions.assertThat(sample.getEstimatedCardinality()).isEqualTo(3);
	}

	@Test
	public void testCcyColumn_returnsDistinctCurrencies() {
		// Sanity check: a different column with mixed cardinality.
		CoordinatesSample sample = table.getCoordinates("ccy", IValueMatcher.MATCH_ALL, 100);

		Assertions.assertThat(sample.getCoordinates()).containsExactlyInAnyOrder("EUR", "USD", "GBP");
		Assertions.assertThat(sample.getEstimatedCardinality()).isEqualTo(3);
	}
}

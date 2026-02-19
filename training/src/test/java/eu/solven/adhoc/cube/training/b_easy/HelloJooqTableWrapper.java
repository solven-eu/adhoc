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
package eu.solven.adhoc.cube.training.b_easy;

import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.assertj.core.api.Assertions;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.Console;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.jdbc.DataSourceBuilder;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.StandardDSLSupplier;

public class HelloJooqTableWrapper {

	private static boolean OPEN_CONSOLE = false;

	@BeforeAll
	public static void openH2Console() throws SQLException {
		if (OPEN_CONSOLE) {
			Console.main();
		}
	}

	@BeforeEach
	public void deleteTables() {

	}

	@Test
	public void ensureNoConsoleByDefault() {
		Assertions.assertThat(OPEN_CONSOLE).isFalse();
	}

	@Test
	public void helloTable() throws SQLException {
		DataSource dataSource = DataSourceBuilder.create()
				.type(JdbcDataSource.class)
				.driverClassName("org.h2.Driver")
				.url("jdbc:h2:mem:test" + Stream.of(
						// `DB_CLOSE_ON_EXIT` disabled the shutdown hook
						"DB_CLOSE_ON_EXIT=FALSE",
						// `DB_CLOSE_DELAY` keeps the DB open even if all connections are closed
						"DB_CLOSE_DELAY=-1",
						// ensure tables consistency
						"DATABASE_TO_LOWER=TRUE").collect(Collectors.joining(";", ";", "")))
				.username("sa")
				.password("")
				.build();

		JooqTableWrapperParameters parameters = JooqTableWrapperParameters.builder()
				.dslSupplier(StandardDSLSupplier.builder().dataSource(dataSource).dialect(SQLDialect.H2).build())
				.tableName("facts")
				.build();
		JooqTableWrapper table = JooqTableWrapper.builder().name("jooq").tableParameters(parameters).build();

		DSLContext dsl = table.makeDsl();
		dsl.createTableIfNotExists("facts")
				.column("color", SQLDataType.VARCHAR)
				.column("ccy", SQLDataType.VARCHAR)
				.column("delta", SQLDataType.FLOAT)
				.execute();

		ITableWrapper tableWrapper = table;

		// We have 3 columns
		Assertions.assertThat(tableWrapper.getColumns())
				.hasSize(3)
				.anySatisfy(c -> Assertions.assertThat(c.getName()).isEqualTo("color"))
				.anySatisfy(c -> Assertions.assertThat(c.getName()).isEqualTo("ccy"))
				.anySatisfy(c -> Assertions.assertThat(c.getName()).isEqualTo("delta"));
	}

	@Test
	public void helloTableWithJoin() throws SQLException {
		DataSource dataSource = DataSourceBuilder.create()
				.type(JdbcDataSource.class)
				.driverClassName("org.h2.Driver")
				.url("jdbc:h2:mem:test" + Stream.of(
						// `DB_CLOSE_ON_EXIT` disabled the shutdown hook
						"DB_CLOSE_ON_EXIT=FALSE",
						// `DB_CLOSE_DELAY` keeps the DB open even if all connections are closed
						"DB_CLOSE_DELAY=-1",
						// ensure tables consistency
						"DATABASE_TO_LOWER=TRUE").collect(Collectors.joining(";", ";", "")))
				.username("sa")
				.password("")
				.build();

		JooqTableWrapperParameters parameters = JooqTableWrapperParameters.builder()
				.dslSupplier(StandardDSLSupplier.builder().dataSource(dataSource).dialect(SQLDialect.H2).build())
				.table(DSL.table("facts JOIN enrichments ON facts.color = enrichments.color"))
				.build();
		JooqTableWrapper table = JooqTableWrapper.builder().name("jooq").tableParameters(parameters).build();

		DSLContext dsl = table.makeDsl();
		dsl.createTableIfNotExists("facts")
				.column("color", SQLDataType.VARCHAR)
				.column("ccy", SQLDataType.VARCHAR)
				.column("delta", SQLDataType.FLOAT)
				.execute();

		int createTableResult = dsl.createTableIfNotExists("enrichments")
				.column("color", SQLDataType.VARCHAR)
				.column("fruit", SQLDataType.VARCHAR)
				.execute();
		Assertions.assertThat(createTableResult).isEqualTo(0);

		// Insert 3 rows
		dsl.insertInto(DSL.table("facts")).set(ImmutableMap.of("color", "blue", "ccy", "EUR")).execute();
		dsl.insertInto(DSL.table("facts")).set(ImmutableMap.of("color", "blue", "ccy", "USD")).execute();
		dsl.insertInto(DSL.table("facts")).set(ImmutableMap.of("color", "red", "ccy", "EUR")).execute();

		ITableWrapper tableWrapper = table;

		// We have 3 columns for base table
		// and 1 from enrichment. The joined field is naturally merged
		Assertions.assertThat(tableWrapper.getColumns())
				.hasSize(4)
				.anySatisfy(c -> Assertions.assertThat(c.getName()).isEqualTo("color"))
				.anySatisfy(c -> Assertions.assertThat(c.getName()).isEqualTo("ccy"))
				.anySatisfy(c -> Assertions.assertThat(c.getName()).isEqualTo("delta"))
				.anySatisfy(c -> Assertions.assertThat(c.getName()).isEqualTo("fruit"));
	}

}

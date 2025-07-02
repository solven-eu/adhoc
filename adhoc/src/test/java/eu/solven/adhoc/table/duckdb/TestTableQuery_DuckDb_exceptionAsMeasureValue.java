/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.table.duckdb;

import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;

public class TestTableQuery_DuckDb_exceptionAsMeasureValue extends ADuckDbJooqTest implements IAdhocTestConstants {

	String tableName = "someTableName";

	TableQuery qK1 = TableQuery.builder().aggregators(Set.of(k1Sum)).build();

	@Override
	public ITableWrapper makeTable() {
		return new JooqTableWrapper(tableName,
				JooqTableWrapperParameters.builder().dslSupplier(dslSupplier).tableName(tableName).build());
	}

	@Test
	public void testTableDoesNotExists() {
		ITabularView result = cube().execute(
				CubeQuery.builder().measure(k1Sum).option(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasEntrySatisfying(Map.of(), measures -> {
			Assertions.assertThat(measures).hasEntrySatisfying(k1Sum.getAlias(), e -> {
				Assertions.assertThat(e)
						.asInstanceOf(InstanceOfAssertFactories.THROWABLE)
						.isInstanceOf(DataAccessException.class)
						.hasStackTraceContaining(
								"Caused by: java.sql.SQLException: Catalog Error: Table with name someTableName does not exist!");
			});
		}).hasSize(1);
	}

	@Test
	public void test_sumOverVarChar() {
		dsl.createTableIfNotExists(tableName).column("k1", SQLDataType.VARCHAR).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k1")).values("someKey").execute();

		ITabularView result = cube().execute(
				CubeQuery.builder().measure(k1Sum).option(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasEntrySatisfying(Map.of(), measures -> {
			Assertions.assertThat(measures).hasEntrySatisfying(k1Sum.getAlias(), e -> {
				Assertions.assertThat(e)
						.asInstanceOf(InstanceOfAssertFactories.THROWABLE)
						.isInstanceOf(DataAccessException.class)
						.hasStackTraceContaining("java.sql.SQLException: Binder Error:"
								+ " No function matches the given name and argument types 'sum(VARCHAR)'."
								+ " You might need to add explicit type casts");
			});
		}).hasSize(1);
	}

	@Test
	public void test_GroupByUnknown() {
		dsl.createTableIfNotExists(tableName).column("k1", SQLDataType.DOUBLE).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k1")).values(12.34).execute();

		ITabularView result = cube().execute(CubeQuery.builder()
				.measure(k1Sum)
				.groupBy(GroupByColumns.named("unknownColumn"))
				.option(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE)
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasEntrySatisfying(Map.of("unknownColumn", "error"), measures -> {
					Assertions.assertThat(measures).hasEntrySatisfying(k1Sum.getAlias(), e -> {
						Assertions.assertThat(e)
								.asInstanceOf(InstanceOfAssertFactories.THROWABLE)
								.isInstanceOf(DataAccessException.class)
								.hasStackTraceContaining("Caused by: java.sql.SQLException: "
										+ "Binder Error: Referenced column \"unknownColumn\" not found in FROM clause!");
					});
				})
				.hasSize(1);

	}

}

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
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.DSLSupplier;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.StandardDSLSupplier;
import eu.solven.adhoc.table.sql.duckdb.DuckDbHelper;

public class TestTableQuery_DuckDb_stopWatch extends ADuckDbJooqTest implements IAdhocTestConstants {

	protected final DSLSupplier dslSupplier =
			StandardDSLSupplier.builder().connection(DuckDbHelper.makeFreshInMemoryDb()).stopWatch().build();
	protected final DSLContext dsl = dslSupplier.getDSLContext();

	String tableName = "someTableName";

	@Override
	public ITableWrapper makeTable() {
		return new JooqTableWrapper(tableName,
				JooqTableWrapperParameters.builder().dslSupplier(dslSupplier).tableName(tableName).build());
	}

	TableQuery qK1 = TableQuery.builder().aggregators(Set.of(k1Sum)).build();

	@BeforeEach
	public void loggerDebug() {
		// Jooq StopWatch prints at DEBUG level: ensure DEBUG logs are printed
		((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(org.jooq.tools.StopWatch.class)).setLevel(Level.DEBUG);
	}

	@Test
	public void testWholeQuery_sumK1() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.INTEGER)
				.execute();
		dsl.insertInto(DSL.table(tableName)).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName)).values("a1", 234).execute();

		forest.addMeasure(k1Sum);
		forest.addMeasure(k1SumSquared);

		ITabularView result = cube().execute(CubeQuery.builder().measure(k1SumSquared.getName()).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1SumSquared.getName(), (long) Math.pow(123 + 234, 2)));
	}
}

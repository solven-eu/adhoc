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
package eu.solven.adhoc.database.duckdb;

import java.sql.Connection;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.dag.AdhocMeasureBag;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.AdhocTestHelper;
import eu.solven.adhoc.database.sql.AdhocJooqTableWrapper;
import eu.solven.adhoc.database.sql.AdhocJooqTableWrapperParameters;
import eu.solven.adhoc.database.sql.DSLSupplier;
import eu.solven.adhoc.database.sql.DuckDbHelper;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.groupby.CalculatedColumn;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.slice.AdhocSliceAsMap;
import eu.solven.adhoc.view.ITabularView;
import eu.solven.adhoc.view.MapBasedTabularView;

public class TestTableQuery_CalculatedColumn implements IAdhocTestConstants {

	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	AdhocQueryEngine aqe = AdhocQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()::post).build();

	String tableName = "someTableName";

	Connection dbConn = DuckDbHelper.makeFreshInMemoryDb();
	AdhocJooqTableWrapper jooqDb = new AdhocJooqTableWrapper(AdhocJooqTableWrapperParameters.builder()
			.dslSupplier(DSLSupplier.fromConnection(() -> dbConn))
			.tableName(tableName)
			.build());

	TableQuery qK1 = TableQuery.builder().aggregators(Set.of(k1Sum)).build();
	DSLContext dsl = jooqDb.makeDsl();

	@Test
	public void testWholeQuery() {
		dsl.createTableIfNotExists(tableName)
				.column("word", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.INTEGER)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("word"), DSL.field("k1")).values("azerty", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("word"), DSL.field("k1")).values("qwerty", 234).execute();

		AdhocMeasureBag measureBag = AdhocMeasureBag.builder().build();
		measureBag.addMeasure(k1Sum);

		ITabularView result =
				aqe.execute(
						AdhocQuery.builder()
								.measure(k1Sum.getName())
								.groupBy(GroupByColumns
										.of(CalculatedColumn.builder().column("first_letter").sql("word[1]").build()))
								.debug(true)
								.build(),
						measureBag,
						jooqDb);
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.keySet().map(AdhocSliceAsMap::getCoordinates).toList())
				.containsExactlyInAnyOrder(Map.of("first_letter", "a"), Map.of("first_letter", "q"));
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("first_letter", "a"), Map.of(k1Sum.getName(), 123L))
				.containsEntry(Map.of("first_letter", "q"), Map.of(k1Sum.getName(), 234L));
	}
}

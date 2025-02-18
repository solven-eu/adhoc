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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.AdhocCubeWrapper;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.AdhocTestHelper;
import eu.solven.adhoc.measure.AdhocMeasureBag;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.storage.MapBasedTabularView;
import eu.solven.adhoc.table.sql.AdhocJooqTableWrapper;
import eu.solven.adhoc.table.sql.AdhocJooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.DuckDbHelper;

@Disabled("TODO")
// https://www.jooq.org/doc/latest/manual/sql-building/queryparts/custom-bindings/
public class TestTableQuery_DuckDb_customType implements IAdhocTestConstants {

	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	AdhocQueryEngine aqe = AdhocQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()::post).build();
	AdhocMeasureBag measureBag = AdhocMeasureBag.builder().name(this.getClass().getName()).build();

	String tableName = "someTableName";

	AdhocJooqTableWrapper jooqDb = new AdhocJooqTableWrapper(tableName,
			AdhocJooqTableWrapperParameters.builder()
					.dslSupplier(DuckDbHelper.inMemoryDSLSupplier())
					.tableName(tableName)
					.build());

	TableQuery qK1 = TableQuery.builder().aggregators(Set.of(k1Sum)).build();
	DSLContext dsl = jooqDb.makeDsl();

	private AdhocCubeWrapper wrapInCube(AdhocMeasureBag measureBag) {
		AdhocQueryEngine aqe = AdhocQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()::post).build();

		return AdhocCubeWrapper.builder().engine(aqe).measures(measureBag).table(jooqDb).engine(aqe).build();
	}

	enum Letter {
		A, B, C
	}

	private void initAndInsert() {
		dsl.createTableIfNotExists(tableName)
				.column("letter", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("letter"), DSL.field("k1")).values("A", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("letter"), DSL.field("k1")).values("B", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("letter"), DSL.field("k1")).values("C", 345).execute();
	}

	@Test
	public void testFilterEnum() {
		initAndInsert();

		measureBag.addMeasure(k1Sum);

		// groupBy `a` with no measure: this is a distinct query on given groupBy
		ITabularView result = wrapInCube(measureBag)
				.execute(AdhocQuery.builder().filter(ColumnFilter.isEqualTo("letter", Letter.A)).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("letter", Letter.A), Map.of(k1Sum.getName(), 0L + 123))
				.hasSize(1);
	}

	@Test
	public void testGroupByEnum() {
		initAndInsert();

		measureBag.addMeasure(k1Sum);

		// groupBy `a` with no measure: this is a distinct query on given groupBy
		ITabularView result = wrapInCube(measureBag).execute(AdhocQuery.builder().groupByAlso("letter").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("letter", Letter.A), Map.of("k1", 0L + 123))
				.containsEntry(Map.of("letter", Letter.B), Map.of("k1", 0L + 234))
				.containsEntry(Map.of("letter", Letter.C), Map.of("k1", 0L + 345))
				.hasSize(3);
	}
}

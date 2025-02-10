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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.AdhocTestHelper;
import eu.solven.adhoc.measure.AdhocMeasureBag;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.storage.MapBasedTabularView;
import eu.solven.adhoc.table.sql.AdhocJooqTableWrapper;
import eu.solven.adhoc.table.sql.AdhocJooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.DSLSupplier;

public class TestTableQuery_DuckDb_withJoin_withAmbiguity implements IAdhocTestConstants {

	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	AdhocQueryEngine aqe = AdhocQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()::post).build();
	AdhocMeasureBag measureBag = AdhocMeasureBag.builder().name("duckdb_withJoin").build();

	String factTable = "someFactTable";
	String joinedTable = "someJoinedName";
	String joinedFurtherTable = "someFurtherJoinedName";

	Table<Record> fromClause = DSL.table(DSL.name(factTable))
			.as("f")
			.leftJoin(DSL.table(DSL.name(joinedTable)).as("p"))
			.using(DSL.field("productId"))
			.leftJoin(DSL.table(DSL.name(joinedFurtherTable)).as("c"))
			.using(DSL.field("countryId"));

	private Connection makeFreshInMemoryDb() {
		try {
			return DriverManager.getConnection("jdbc:duckdb:");
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}

	Connection dbConn = makeFreshInMemoryDb();
	AdhocJooqTableWrapper jooqDb = new AdhocJooqTableWrapper(factTable,
			AdhocJooqTableWrapperParameters.builder()
					.dslSupplier(DSLSupplier.fromConnection(() -> dbConn))
					.table(fromClause)
					.build());

	TableQuery qK1 = TableQuery.builder().aggregators(Set.of(k1Sum)).build();
	DSLContext dsl = jooqDb.makeDsl();

	@Test
	public void testTableDoesNotExists() {
		Assertions.assertThatThrownBy(() -> jooqDb.openDbStream(qK1).toList())
				.isInstanceOf(DataAccessException.class)
				.hasMessageContaining("Table with name someFactTable does not exist!");
	}

	private void initTables() {
		dsl.createTableIfNotExists(factTable)
				.column("k1", SQLDataType.DOUBLE)
				.column("productId", SQLDataType.VARCHAR)
				.column("name", SQLDataType.VARCHAR)
				.execute();
		dsl.createTableIfNotExists(joinedTable)
				.column("productId", SQLDataType.VARCHAR)
				.column("productName", SQLDataType.VARCHAR)
				.column("countryId", SQLDataType.VARCHAR)
				.column("name", SQLDataType.VARCHAR)
				.execute();
		dsl.createTableIfNotExists(joinedFurtherTable)
				.column("countryId", SQLDataType.VARCHAR)
				.column("countryName", SQLDataType.VARCHAR)
				.column("name", SQLDataType.VARCHAR)
				.execute();
	}

	private void insertData() {
		// Carot is fully joined
		dsl.insertInto(DSL.table(factTable), DSL.field("k1"), DSL.field("productId"), DSL.field("name"))
				.values(123, "carot", "fName")
				.execute();
		dsl.insertInto(DSL.table(joinedTable),
				DSL.field("productId"),
				DSL.field("productName"),
				DSL.field("countryId"),
				DSL.field("name")).values("carot", "Carotte", "FRA", "pName").execute();
		dsl.insertInto(DSL.table(joinedFurtherTable),
				DSL.field("countryId"),
				DSL.field("countryName"),
				DSL.field("name")).values("FRA", "France", "cName").execute();

		// Banana is not joined
		dsl.insertInto(DSL.table(factTable), DSL.field("k1"), DSL.field("productId")).values(234, "banana").execute();
	}

	@Test
	public void testEmptyDb() {
		initTables();

		List<Map<String, ?>> dbStream = jooqDb.openDbStream(qK1).toList();

		Assertions.assertThat(dbStream).isEmpty();

		Assertions.assertThat(jooqDb.getColumns())
				.containsEntry("countryId", String.class)
				.containsEntry("countryName", String.class)
				.containsEntry("productId", String.class)
				.containsEntry("productName", String.class)
				// `name` is very ambiguous, as it is provided by the base table and 2 JOINed tables
				.containsEntry("name", String.class)
				.containsEntry("k1", Double.class)
				.hasSize(6);
	}

	@Test
	public void testGrandTotal() {
		initTables();
		insertData();

		measureBag.addMeasure(k1Sum);

		{
			ITabularView result =
					aqe.execute(AdhocQuery.builder().measure(k1Sum.getName()).build(), measureBag, jooqDb);
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 123 + 234));
		}
	}

	@Test
	public void testByNotJoined() {
		initTables();
		insertData();

		measureBag.addMeasure(k1Sum);

		{
			ITabularView result = aqe.execute(
					AdhocQuery.builder().measure(k1Sum.getName()).groupByAlso("productId").debug(true).build(),
					measureBag,
					jooqDb);
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("productId", "carot"), Map.of(k1Sum.getName(), 0L + 123))
					.containsEntry(Map.of("productId", "banana"), Map.of(k1Sum.getName(), 0L + 234))
					.hasSize(2);
		}
	}

	@Test
	public void testByJoinedField() {
		initTables();
		insertData();

		measureBag.addMeasure(k1Sum);

		{
			ITabularView result = aqe.execute(
					AdhocQuery.builder().measure(k1Sum.getName()).groupByAlso("p.productName").debug(true).build(),
					measureBag,
					jooqDb);
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("p.productName", "Carotte"), Map.of(k1Sum.getName(), 0L + 123))
					.containsEntry(Map.of("p.productName", "NULL"), Map.of(k1Sum.getName(), 0L + 234))
					.hasSize(2);
		}
	}

	@Test
	public void testByJoinedKey_qualifiedByFrom() {
		initTables();
		insertData();

		measureBag.addMeasure(k1Sum);

		{
			ITabularView result = aqe.execute(
					AdhocQuery.builder().measure(k1Sum.getName()).groupByAlso("f.productId").debug(true).build(),
					measureBag,
					jooqDb);
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("f.productId", "carot"), Map.of(k1Sum.getName(), 0L + 123))
					.containsEntry(Map.of("f.productId", "banana"), Map.of(k1Sum.getName(), 0L + 234))
					.hasSize(2);
		}
	}

	@Test
	public void testByJoinedKey_qualifiedByTo() {
		initTables();
		insertData();

		measureBag.addMeasure(k1Sum);

		{
			ITabularView result = aqe.execute(
					AdhocQuery.builder().measure(k1Sum.getName()).groupByAlso("p.productId").debug(true).build(),
					measureBag,
					jooqDb);
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("p.productId", "carot"), Map.of(k1Sum.getName(), 0L + 123))
					.containsEntry(Map.of("p.productId", "NULL"), Map.of(k1Sum.getName(), 0L + 234))
					.hasSize(2);
		}
	}

	@Test
	public void testByJoinedKey_notQualified() {
		initTables();
		insertData();

		measureBag.addMeasure(k1Sum);

		{
			ITabularView result = aqe.execute(
					AdhocQuery.builder().measure(k1Sum.getName()).groupByAlso("productId").debug(true).build(),
					measureBag,
					jooqDb);
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("productId", "carot"), Map.of(k1Sum.getName(), 0L + 123))
					.containsEntry(Map.of("productId", "banana"), Map.of(k1Sum.getName(), 0L + 234))
					.hasSize(2);
		}
	}

	@Test
	public void testByJoinedTwice() {
		initTables();
		insertData();

		measureBag.addMeasure(k1Sum);

		{
			ITabularView result = aqe.execute(
					AdhocQuery.builder().measure(k1Sum.getName()).groupByAlso("c.countryName").debug(true).build(),
					measureBag,
					jooqDb);
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("c.countryName", "France"), Map.of(k1Sum.getName(), 0L + 123))
					.containsEntry(Map.of("c.countryName", "NULL"), Map.of(k1Sum.getName(), 0L + 234))
					.hasSize(2);
		}
	}
}

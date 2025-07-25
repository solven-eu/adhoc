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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.transformator.MapWithNulls;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;

public class TestTableQuery_DuckDb_withJoin extends ADuckDbJooqTest implements IAdhocTestConstants {

	String factTable = "someFactTable";
	String joinedTable = "someJoinedName";
	String joinedFurtherTable = "someFurtherJoinedName";

	Table<Record> fromClause = DSL.table(DSL.name(factTable))
			.as("f")
			.leftJoin(DSL.table(DSL.name(joinedTable)).as("p"))
			.using(DSL.field("productId"))
			.leftJoin(DSL.table(DSL.name(joinedFurtherTable)).as("c"))
			.using(DSL.field("countryId"));

	@Override
	public ITableWrapper makeTable() {
		return new JooqTableWrapper(factTable,
				JooqTableWrapperParameters.builder().dslSupplier(dslSupplier).table(fromClause).build());
	}

	TableQuery qK1 = TableQuery.builder().aggregators(Set.of(k1Sum)).build();

	@Test
	public void testTableDoesNotExists() {
		Assertions.assertThatThrownBy(() -> table().streamSlices(qK1).toList())
				.isInstanceOf(DataAccessException.class)
				.hasMessageContaining("Table with name someFactTable does not exist!");
	}

	private void initTables() {
		dsl.createTableIfNotExists(factTable)
				.column("k1", SQLDataType.INTEGER)
				.column("productId", SQLDataType.VARCHAR)
				.execute();
		dsl.createTableIfNotExists(joinedTable)
				.column("productId", SQLDataType.VARCHAR)
				.column("productName", SQLDataType.VARCHAR)
				.column("countryId", SQLDataType.VARCHAR)
				.execute();
		dsl.createTableIfNotExists(joinedFurtherTable)
				.column("countryId", SQLDataType.VARCHAR)
				.column("countryName", SQLDataType.VARCHAR)
				.execute();
	}

	private void insertData() {
		// Carot is fully joined
		dsl.insertInto(DSL.table(factTable), DSL.field("k1"), DSL.field("productId")).values(123, "carot").execute();
		dsl.insertInto(DSL.table(joinedTable), DSL.field("productId"), DSL.field("productName"), DSL.field("countryId"))
				.values("carot", "Carotte", "FRA")
				.execute();
		dsl.insertInto(DSL.table(joinedFurtherTable), DSL.field("countryId"), DSL.field("countryName"))
				.values("FRA", "France")
				.execute();

		// Banana is not joined
		dsl.insertInto(DSL.table(factTable), DSL.field("k1"), DSL.field("productId")).values(234, "banana").execute();
	}

	@Test
	public void testEmptyDb() {
		initTables();

		List<Map<String, ?>> dbStream = table().streamSlices(qK1).toList();

		// It seems a legal SQL behavior: a groupBy with `null` is created even if there is not a single matching row
		// Given `null` is filtered by TabularRecordOverMaps
		Assertions.assertThat(dbStream).contains(Map.of()).hasSize(1);

		Assertions.assertThat(table().getColumnTypes())
				.containsEntry("countryId", String.class)
				.containsEntry("countryName", String.class)
				.containsEntry("productId", String.class)
				.containsEntry("productName", String.class)
				.containsEntry("k1", Integer.class)
				.hasSize(5);
	}

	@Test
	public void testGrandTotal() {
		initTables();
		insertData();

		forest.addMeasure(k1Sum);

		{
			ITabularView result = cube().execute(CubeQuery.builder().measure(k1Sum.getName()).build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 123 + 234));
		}
	}

	@Test
	public void testByNotJoined() {
		initTables();
		insertData();

		forest.addMeasure(k1Sum);

		{
			ITabularView result =
					cube().execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso("productId").build());
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

		forest.addMeasure(k1Sum);

		{
			ITabularView result =
					cube().execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso("p.productName").build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("p.productName", "Carotte"), Map.of(k1Sum.getName(), 0L + 123))
					.containsEntry(MapWithNulls.of("p.productName", null), Map.of(k1Sum.getName(), 0L + 234))
					.hasSize(2);
		}
	}

	@Test
	public void testByJoinedKey_qualifiedByFrom() {
		initTables();
		insertData();

		forest.addMeasure(k1Sum);

		{
			ITabularView result =
					cube().execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso("f.productId").build());
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

		forest.addMeasure(k1Sum);

		{
			ITabularView result =
					cube().execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso("p.productId").build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("p.productId", "carot"), Map.of(k1Sum.getName(), 0L + 123))
					.containsEntry(MapWithNulls.of("p.productId", null), Map.of(k1Sum.getName(), 0L + 234))
					.hasSize(2);
		}
	}

	@Test
	public void testByJoinedKey_notQualified() {
		initTables();
		insertData();

		forest.addMeasure(k1Sum);

		{
			ITabularView result =
					cube().execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso("productId").build());
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

		forest.addMeasure(k1Sum);

		{
			ITabularView result =
					cube().execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso("c.countryName").build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("c.countryName", "France"), Map.of(k1Sum.getName(), 0L + 123))
					.containsEntry(MapWithNulls.of("c.countryName", null), Map.of(k1Sum.getName(), 0L + 234))
					.hasSize(2);
		}
	}

	@Test
	public void testCountAsterisk_grandTotal() {
		initTables();
		insertData();

		forest.addMeasure(countAsterisk);

		{
			ITabularView result = cube()
					.execute(CubeQuery.builder().measure(countAsterisk.getName()).groupByAlso("productId").build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("productId", "carot"), Map.of(countAsterisk.getName(), 0L + 1L))
					.containsEntry(Map.of("productId", "banana"), Map.of(countAsterisk.getName(), 0L + 1L))
					.hasSize(2);
		}
	}

	@Test
	public void testCountAsterisk_ByJoinedTwice() {
		initTables();
		insertData();

		forest.addMeasure(countAsterisk);

		{
			ITabularView result = cube()
					.execute(CubeQuery.builder().measure(countAsterisk.getName()).groupByAlso("c.countryName").build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("c.countryName", "France"), Map.of(countAsterisk.getName(), 0L + 1L))
					.containsEntry(MapWithNulls.of("c.countryName", null), Map.of(countAsterisk.getName(), 0L + 1L))
					.hasSize(2);
		}
	}
}

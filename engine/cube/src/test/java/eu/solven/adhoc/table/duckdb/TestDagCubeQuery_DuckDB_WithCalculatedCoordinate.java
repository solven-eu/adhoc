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

import java.time.LocalDate;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.column.coordinate.CalculatedCoordinate;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.value.ComparingMatcher;
import eu.solven.adhoc.model.column.ColumnWithCalculatedCoordinates;
import eu.solven.adhoc.model.query.groupby.GroupByColumns;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;

public class TestDagCubeQuery_DuckDB_WithCalculatedCoordinate extends ATestDagDuckDb implements IAdhocTestConstants {

	String tableName = "someTableName";

	@Override
	public ITableWrapper makeTable() {
		return new JooqTableWrapper(tableName,
				DuckDBHelper.parametersBuilder(dslSupplier).tableName(tableName).build());
	}

	LocalDate today = LocalDate.now();

	@BeforeEach
	public void initDataAndMeasures() {
		dsl.createTableIfNotExists(tableName)
				.column("color", SQLDataType.VARCHAR)
				.column("d", SQLDataType.DATE)
				.column("k1", SQLDataType.INTEGER)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("color"), DSL.field("d"), DSL.field("k1"))
				.values("blue", today, 123)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("color"), DSL.field("d"), DSL.field("k1"))
				.values("red", today.minusYears(1), 234)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("color"), DSL.field("d"), DSL.field("k1"))
				.values("green", today, 345)
				.execute();

		forest.addMeasure(k1Sum);
	}

	@Test
	public void testGetColumns() {
		Assertions.assertThat(cube().getColumns()).anySatisfy(c -> {
			Assertions.assertThat(c.getName()).isEqualTo("k1");
			Assertions.assertThat(c.getType()).isEqualTo(Integer.class);
		}).anySatisfy(c -> {
			Assertions.assertThat(c.getName()).isEqualTo("color");
			Assertions.assertThat(c.getType()).isEqualTo(String.class);
		}).anySatisfy(c -> {
			Assertions.assertThat(c.getName()).isEqualTo("d");
			Assertions.assertThat(c.getType()).isEqualTo(LocalDate.class);
		}).hasSize(3);
	}

	@Test
	public void test_GroupByDate() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure(k1Sum)
				.groupBy(GroupByColumns.of(ColumnWithCalculatedCoordinates.builder()
						.column("d")
						.calculatedCoordinate(
								CalculatedCoordinate.builder().coordinate("*").filter(ISliceFilter.MATCH_ALL).build())
						.build()))
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("d", "*"), Map.of(k1Sum.getName(), 0L + 123 + 234 + 345))
				.containsEntry(Map.of("d", today), Map.of(k1Sum.getName(), 0L + 123 + 345))
				.containsEntry(Map.of("d", today.minusYears(1)), Map.of(k1Sum.getName(), 0L + 234))
				.hasSize(3);
	}

	// Adds calculated coordinates on 2 different columns: it leads to a cartesian product
	@Test
	public void test_GroupByDateAndColor() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure(k1Sum)
				.groupBy(GroupByColumns.of(
						ColumnWithCalculatedCoordinates.builder()
								.column("d")
								.calculatedCoordinate(CalculatedCoordinate.star())
								.build(),
						ColumnWithCalculatedCoordinates.builder()
								.column("color")
								.calculatedCoordinate(CalculatedCoordinate.star())
								.build()))
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("color", "*", "d", "*"), Map.of(k1Sum.getName(), 0L + 123 + 234 + 345))
				.containsEntry(Map.of("color", "*", "d", today), Map.of(k1Sum.getName(), 0L + 123 + 345))
				.containsEntry(Map.of("color", "*", "d", today.minusYears(1)), Map.of(k1Sum.getName(), 0L + 234))
				.containsEntry(Map.of("color", "blue", "d", "*"), Map.of(k1Sum.getName(), 0L + 123))
				.containsEntry(Map.of("color", "red", "d", "*"), Map.of(k1Sum.getName(), 0L + 234))
				.containsEntry(Map.of("color", "green", "d", "*"), Map.of(k1Sum.getName(), 0L + 345))
				.containsEntry(Map.of("color", "blue", "d", today), Map.of(k1Sum.getName(), 0L + 123))
				.containsEntry(Map.of("color", "red", "d", today.minusYears(1)), Map.of(k1Sum.getName(), 0L + 234))
				.containsEntry(Map.of("color", "green", "d", today), Map.of(k1Sum.getName(), 0L + 345))
				.hasSize(9);
	}

	/**
	 * Conflict case: a calculated coordinate named {@code blue} on the {@code color} groupBy whose filter narrows the
	 * slice to {@code d=today}. The calculated coordinate name collides with a real coordinate present in the
	 * underlying table (the natural {@code color=blue} row). The engine's collision-resolution rule is to suppress the
	 * natural row whose value matches a declared calculated-coordinate name — the calculated coordinate becomes
	 * authoritative for its slice key. With this scenario the calculated value ({@code SUM(k1) WHERE d=today} = blue +
	 * green = {@code 123 + 345 = 468}) is intentionally different from the natural blue value ({@code 123}) so a
	 * regression that re-introduces the natural row would surface as a value mismatch rather than a coincidental match.
	 */
	@Test
	public void test_GroupByColor_calculatedCoordinateConflictsWithRealCoordinate() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure(k1Sum)
				.groupBy(GroupByColumns.of(ColumnWithCalculatedCoordinates.builder()
						.column("color")
						.calculatedCoordinate(CalculatedCoordinate.builder()
								.coordinate("blue")
								.filter(ColumnFilter.matchEq("d", today))
								.build())
						.build()))
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				// Calculated `blue` row: sum where d=today => blue(123) + green(345) = 468. Replaces the
				// natural blue=123 row (suppressed by the engine's NOT IN filter on the natural query).
				.containsEntry(Map.of("color", "blue"), Map.of(k1Sum.getName(), 0L + 123 + 345))
				.containsEntry(Map.of("color", "red"), Map.of(k1Sum.getName(), 0L + 234))
				.containsEntry(Map.of("color", "green"), Map.of(k1Sum.getName(), 0L + 345))
				.hasSize(3);
	}

	@Test
	public void test_GroupByDate_filterSmallDates() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure(k1Sum)
				.groupBy(GroupByColumns.of(ColumnWithCalculatedCoordinates.builder()
						.column("d")
						.calculatedCoordinate(
								CalculatedCoordinate.builder().coordinate("*").filter(ISliceFilter.MATCH_ALL).build())
						.build()))
				.filter(ColumnFilter.match("d", ComparingMatcher.greaterThanOrEqual(today)))
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("d", "*"), Map.of(k1Sum.getName(), 0L + 123 + 345))
				.containsEntry(Map.of("d", today), Map.of(k1Sum.getName(), 0L + 123 + 345))
				.hasSize(2);
	}

}

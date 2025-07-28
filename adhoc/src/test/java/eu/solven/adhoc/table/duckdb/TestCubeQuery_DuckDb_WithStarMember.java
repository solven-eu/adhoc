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
import eu.solven.adhoc.column.ColumnWithCalculatedCoordinates;
import eu.solven.adhoc.coordinate.CalculatedCoordinate;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.value.ComparingMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;

public class TestCubeQuery_DuckDb_WithStarMember extends ADuckDbJooqTest implements IAdhocTestConstants {

	String tableName = "someTableName";

	@Override
	public ITableWrapper makeTable() {
		return new JooqTableWrapper(tableName,
				JooqTableWrapperParameters.builder().dslSupplier(dslSupplier).tableName(tableName).build());
	}

	LocalDate today = LocalDate.now();

	@BeforeEach
	public void initDataAndMeasures() {
		dsl.createTableIfNotExists(tableName).column("d", SQLDataType.DATE).column("k1", SQLDataType.INTEGER).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("d"), DSL.field("k1")).values(today, 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("d"), DSL.field("k1"))
				.values(today.minusYears(1), 234)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("d"), DSL.field("k1")).values(today, 345).execute();

		forest.addMeasure(k1Sum);
	}

	@Test
	public void testGetColumns() {
		Assertions.assertThat(cube().getColumns()).anySatisfy(c -> {
			Assertions.assertThat(c.getName()).isEqualTo("k1");
			Assertions.assertThat(c.getType()).isEqualTo(Integer.class);
		}).anySatisfy(c -> {
			Assertions.assertThat(c.getName()).isEqualTo("d");
			Assertions.assertThat(c.getType()).isEqualTo(LocalDate.class);
		}).hasSize(2);
	}

	@Test
	public void test_GroupByDate() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure(k1Sum)
				.groupBy(GroupByColumns.of(ColumnWithCalculatedCoordinates.builder()
						.column("d")
						.calculatedCoordinate(
								CalculatedCoordinate.builder().coordinate("*").filter(IAdhocFilter.MATCH_ALL).build())
						.build()))
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("d", "*"), Map.of(k1Sum.getName(), 0L + 123 + 234 + 345))
				.containsEntry(Map.of("d", today), Map.of(k1Sum.getName(), 0L + 123 + 345))
				.containsEntry(Map.of("d", today.minusYears(1)), Map.of(k1Sum.getName(), 0L + 234))
				.hasSize(3);
	}

	@Test
	public void test_GroupByDate_filterSmallDates() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure(k1Sum)
				.groupBy(GroupByColumns.of(ColumnWithCalculatedCoordinates.builder()
						.column("d")
						.calculatedCoordinate(
								CalculatedCoordinate.builder().coordinate("*").filter(IAdhocFilter.MATCH_ALL).build())
						.build()))
				.filter(ColumnFilter.isMatching("d", ComparingMatcher.greaterThanOrEqual(today)))
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("d", "*"), Map.of(k1Sum.getName(), 0L + 123 + 345))
				.containsEntry(Map.of("d", today), Map.of(k1Sum.getName(), 0L + 123 + 345))
				.hasSize(2);
	}

}

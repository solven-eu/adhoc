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

import org.assertj.core.api.Assertions;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;

public class TestCubeQuery_DuckDb_Integer extends ADuckDbJooqTest implements IAdhocTestConstants {

	String tableName = "someTableName";

	@Override
	public ITableWrapper makeTable() {
		return new JooqTableWrapper(tableName,
				JooqTableWrapperParameters.builder().dslSupplier(dslSupplier).tableName(tableName).build());
	}

	@BeforeEach
	public void initDataAndMeasures() {
		dsl.createTableIfNotExists(tableName)
				.column("I", SQLDataType.BIGINT)
				.column("k1", SQLDataType.INTEGER)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("I"), DSL.field("k1")).values(1, 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("I"), DSL.field("k1")).values(12, 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("I"), DSL.field("k1")).values(21, 345).execute();

		forest.addMeasure(k1Sum);
	}

	@Test
	public void testGetColumns() {
		Assertions.assertThat(cube().getColumns()).anySatisfy(c -> {
			Assertions.assertThat(c.getName()).isEqualTo("k1");
			Assertions.assertThat(c.getType()).isEqualTo(Integer.class);
		}).anySatisfy(c -> {
			Assertions.assertThat(c.getName()).isEqualTo("I");
			Assertions.assertThat(c.getType()).isEqualTo(Long.class);
		}).hasSize(2);
	}

	@Test
	public void test_GroupByInteger() {
		ITabularView result = cube().execute(CubeQuery.builder().measure(k1Sum).groupByAlso("I").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("I", 1L), Map.of(k1Sum.getName(), 0L + 123))
				.containsEntry(Map.of("I", 12L), Map.of(k1Sum.getName(), 0L + 234))
				.containsEntry(Map.of("I", 21L), Map.of(k1Sum.getName(), 0L + 345))
				.hasSize(3);
	}

	@Test
	public void test_filterLike() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure(k1Sum)
				.filter(ColumnFilter.builder().column("I").matching(LikeMatcher.matching("1%")).build())
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 123 + 234))
				.hasSize(1);
	}

	@Test
	public void test_filterLike_groupBy() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure(k1Sum)
				.filter(ColumnFilter.builder().column("I").matching(LikeMatcher.matching("1%")).build())
				.groupByAlso("I")
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("I", 1L), Map.of(k1Sum.getName(), 0L + 123))
				.containsEntry(Map.of("I", 12L), Map.of(k1Sum.getName(), 0L + 234))
				.hasSize(2);
	}

}

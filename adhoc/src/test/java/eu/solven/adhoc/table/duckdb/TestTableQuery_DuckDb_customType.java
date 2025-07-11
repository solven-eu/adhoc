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
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.CubeWrapper.CubeWrapperBuilder;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.AdhocTestHelper;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.transcoder.value.ICustomTypeManager;
import eu.solven.adhoc.util.IAdhocEventBus;

//@Disabled("TODO")
// https://www.jooq.org/doc/latest/manual/sql-building/queryparts/custom-bindings/
public class TestTableQuery_DuckDb_customType extends ADuckDbJooqTest implements IAdhocTestConstants {

	String tableName = "someTableName";

	@Override
	public ITableWrapper makeTable() {
		return new JooqTableWrapper(tableName,
				JooqTableWrapperParameters.builder().dslSupplier(dslSupplier).tableName(tableName).build());
	}

	@Override
	public CubeWrapperBuilder makeCube() {
		IAdhocEventBus adhocEventBus = AdhocTestHelper.eventBus()::post;
		CubeQueryEngine engine = CubeQueryEngine.builder().eventBus(adhocEventBus).build();

		ICustomTypeManager customTypeManager = new ICustomTypeManager() {
			@Override
			public Map<String, Class<?>> getColumnTypes() {
				return Map.of("letter", Letter.class);
			}

			@Override
			public boolean mayTranscode(String column) {
				return "letter".equals(column);
			}

			@Override
			public IValueMatcher toTable(String column, IValueMatcher valueMatcher) {
				// BEWARE how can we know this is a `isMagic` filter and not some Adhoc matcher?
				// BEWARE Should we transcode it, e.g. by integrating the `parseEnum` logic?
				return o -> valueMatcher.match(fromTable(column, o));
			}

			@Override
			public Object toTable(String column, Object coordinate) {
				if ("letter".equals(column) && coordinate instanceof Letter letter) {
					return letter.name();
				}
				return coordinate;
			}

			@Override
			public Object fromTable(String column, Object coordinate) {
				if ("letter".equals(column) && coordinate instanceof String rawLetter) {
					return Letter.valueOf(rawLetter);
				}
				return coordinate;
			}
		};
		ColumnsManager columnsManager =
				ColumnsManager.builder().eventBus(adhocEventBus).customTypeManager(customTypeManager).build();
		return CubeWrapper.builder()
				.engine(engine)
				.forest(forest)
				.table(table())
				.engine(engine)
				.columnsManager(columnsManager);
	}

	enum Letter {
		A, B, C;

		public boolean isMagic() {
			return this == A || this == C;
		}
	}

	private void initAndInsert() {
		dsl.createTableIfNotExists(tableName)
				.column("letter", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.INTEGER)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("letter"), DSL.field("k1")).values("A", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("letter"), DSL.field("k1")).values("B", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("letter"), DSL.field("k1")).values("C", 345).execute();
	}

	@Test
	public void testGetColumns() {
		initAndInsert();

		Assertions.assertThat(cube().getColumnTypes())
				.hasSize(2)
				.containsEntry("letter", Letter.class)
				.containsEntry("k1", Integer.class);
	}

	@Test
	public void testFilterEnum_equals() {
		initAndInsert();

		forest.addMeasure(k1Sum);

		// groupBy `a` with no measure: this is a distinct query on given groupBy
		ITabularView result = cube()
				.execute(CubeQuery.builder().filter(ColumnFilter.isEqualTo("letter", Letter.A)).measure(k1Sum).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 123))
				.hasSize(1);
	}

	// This test is for complex filters. These may happens even on native types (e.g. `VARCHAR`) but typically happens
	// in project for complex types
	@Test
	public void testFilterEnum_complex() {
		initAndInsert();

		forest.addMeasure(k1Sum);

		// this is a customValue matcher: it is typically not resolvable by the table
		IValueMatcher customValueMatcher =
				FilterHelpers.wrapWithToString(letter -> letter instanceof Letter l && l.isMagic(), () -> "isMagic");
		ColumnFilter customFilter = ColumnFilter.builder().column("letter").valueMatcher(customValueMatcher).build();

		// Not distinct as we query a column which is later suppressed, for the sake of lateFiltering
		Assertions
				.assertThat(table().streamSlices(TableQuery.builder().filter(customFilter).build()).isDistinctSlices())
				.isFalse();

		ITabularView result = cube().execute(CubeQuery.builder().filter(customFilter).measure(k1Sum).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 123 + 345))
				.hasSize(1);
	}

	@Test
	public void testGroupByEnum() {
		initAndInsert();

		forest.addMeasure(k1Sum);

		// groupBy `a` with no measure: this is a distinct query on given groupBy
		ITabularView result = cube().execute(CubeQuery.builder().groupByAlso("letter").measure(k1Sum).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("letter", Letter.A), Map.of("k1", 0L + 123))
				.containsEntry(Map.of("letter", Letter.B), Map.of("k1", 0L + 234))
				.containsEntry(Map.of("letter", Letter.C), Map.of("k1", 0L + 345))
				.hasSize(3);
	}
}

/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.model.measure.Filtrator;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;

/**
 * Counterpart of {@link TestDagTableQuery_DuckDB_customAggregation} for filters: exercises a custom
 * {@link IValueMatcher} that the {@link ITableWrapper} cannot translate into SQL (default
 * {@code JooqTableQueryFactory.onCustomCondition} returns {@code null}), so the matcher falls back to engine-side
 * post-filtering of the table's raw rows.
 */
public class TestDagTableQuery_DuckDB_customFilter extends ATestDagDuckDb implements IAdhocTestConstants {

	String tableName = "someTableName";

	@Override
	public ITableWrapper makeTable() {
		return new JooqTableWrapper(tableName,
				DuckDBHelper.parametersBuilder(dslSupplier).tableName(tableName).build());
	}

	// A predicate the SQL layer cannot translate: matches strings strictly shorter than 4 characters.
	// Wrapped via FilterHelpers.wrapWithToString so it stays a vanilla IValueMatcher (no jOOQ extension hook),
	// forcing the engine into the non-pushdown / post-filter path.
	IValueMatcher isShort =
			FilterHelpers.wrapWithToString(value -> value instanceof String s && s.length() < 4, () -> "isShort");

	@BeforeEach
	public void initAndInsert() {
		dsl.createTableIfNotExists(tableName)
				.column("color", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.INTEGER)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("color"), DSL.field("k1")).values("blue", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("color"), DSL.field("k1")).values("blue", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("color"), DSL.field("k1")).values("red", 345).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("color"), DSL.field("k1")).values("red", 456).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("color"), DSL.field("k1")).values("green", 567).execute();

		forest.addMeasure(k1Sum);
	}

	@Test
	public void testGetColumns() {
		Assertions.assertThat(cube().getColumnTypes())
				.hasSize(2)
				.containsEntry("color", String.class)
				.containsEntry("k1", Integer.class);
	}

	@Test
	public void testGrandTotal_noFilter() {
		ITabularView result = cube().execute(CubeQuery.builder().measure(k1Sum).build());

		Assertions.assertThat(MapBasedTabularView.load(result).getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 123 + 234 + 345 + 456 + 567))
				.hasSize(1);
	}

	@Test
	public void testGrandTotal_customFilter() {
		// Only `red` (3 chars) matches; `blue`, `green` do not.
		ColumnFilter shortColors = ColumnFilter.builder().column("color").valueMatcher(isShort).build();

		ITabularView result =
				cube().execute(CubeQuery.builder().filter(shortColors).measure(k1Sum).explain(true).build());

		Assertions.assertThat(MapBasedTabularView.load(result).getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 345 + 456))
				.hasSize(1);
	}

	@Test
	public void testGroupBy_customFilter() {
		ColumnFilter shortColors = ColumnFilter.builder().column("color").valueMatcher(isShort).build();

		ITabularView result =
				cube().execute(CubeQuery.builder().filter(shortColors).groupByAlso("color").measure(k1Sum).build());

		Assertions.assertThat(MapBasedTabularView.load(result).getCoordinatesToValues())
				.containsEntry(Map.of("color", "red"), Map.of(k1Sum.getName(), 0L + 345 + 456))
				.hasSize(1);
	}

	/**
	 * Custom {@link IValueMatcher} carried by a {@link Filtrator}'s filter — i.e. it lands on a per-aggregator
	 * {@code FILTER (WHERE ...)} clause rather than the shared {@code WHERE}. Querying both the bare {@code k1} and the
	 * Filtrator-wrapped measure forces the engine to emit a {@code TableQueryV4} with two {@code FilteredAggregator}s
	 * whose filters differ; the per-aggregator non-pushdown matcher is exactly the
	 * {@code FilteredAggregator::getFilter} branch that the recently-broadened {@code TableQueryV4.getFilteredColumns}
	 * (and the reducer's {@code hoistableColumns}) must cover.
	 */
	@Test
	public void testFiltrator_customFilter_bothMeasures() {
		ColumnFilter shortColors = ColumnFilter.builder().column("color").valueMatcher(isShort).build();
		forest.addMeasure(Filtrator.builder().name("k1.short").underlying(k1Sum.getName()).filter(shortColors).build());

		ITabularView result = cube().execute(CubeQuery.builder().measure(k1Sum.getName(), "k1.short").build());

		Assertions.assertThat(MapBasedTabularView.load(result).getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(),
						Map.of(k1Sum.getName(), 0L + 123 + 234 + 345 + 456 + 567, "k1.short", 0L + 345 + 456));
	}

	@Test
	public void testGroupBy_customFilter_andTranslatableFilter() {
		// `color IN ('blue', 'red')` is pushed down; `isShort` is post-filtered. Intersection is `red` only.
		ColumnFilter shortColors = ColumnFilter.builder().column("color").valueMatcher(isShort).build();
		ColumnFilter blueOrRed =
				ColumnFilter.builder().column("color").matchIn(java.util.Set.of("blue", "red")).build();

		ITabularView result = cube().execute(CubeQuery.builder()
				.filter(AndFilter.and(blueOrRed, shortColors))
				.groupByAlso("color")
				.measure(k1Sum)
				.build());

		Assertions.assertThat(MapBasedTabularView.load(result).getCoordinatesToValues())
				.containsEntry(Map.of("color", "red"), Map.of(k1Sum.getName(), 0L + 345 + 456))
				.hasSize(1);
	}
}

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

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.AdhocTestHelper;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.value.ComparingMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.StringMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.duckdb.DuckDbHelper;

public class TestTableQuery_DuckDb extends ADagTest implements IAdhocTestConstants {

	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	String tableName = "someTableName";

	JooqTableWrapper table = new JooqTableWrapper(tableName,
			JooqTableWrapperParameters.builder()
					.dslSupplier(DuckDbHelper.inMemoryDSLSupplier())
					.tableName(tableName)
					.build());

	TableQuery qK1 = TableQuery.builder().aggregators(Set.of(k1Sum)).build();
	DSLContext dsl = table.makeDsl();

	private CubeWrapper wrapInCube(IMeasureForest forest) {
		CubeQueryEngine aqe = CubeQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()::post).build();

		return CubeWrapper.builder().engine(aqe).forest(forest).table(table).engine(aqe).build();
	}

	@Override
	public void feedTable() {
		// No standard feeding in this class
	}

	@Test
	public void testTableDoesNotExists() {
		Assertions.assertThatThrownBy(() -> table.streamSlices(qK1).toList()).isInstanceOf(DataAccessException.class);
	}

	@Test
	public void testEmptyDb() {
		dsl.createTableIfNotExists(tableName).column("k1", SQLDataType.DOUBLE).execute();

		List<Map<String, ?>> tableStream = table.streamSlices(qK1).toList();

		// It seems a legal SQL behavior: a groupBy with `null` is created even if there is not a single matching row
		Assertions.assertThat(tableStream).contains(Map.of()).hasSize(1);
	}

	@Test
	public void testReturnAll() {
		dsl.createTableIfNotExists(tableName).column("k1", SQLDataType.DOUBLE).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k1")).values(123).execute();

		List<Map<String, ?>> tableStream = table.streamSlices(qK1).toList();

		Assertions.assertThat(tableStream).hasSize(1).contains(Map.of("k1", BigDecimal.valueOf(0D + 123)));
	}

	@Test
	public void test_sumOverVarChar() {
		dsl.createTableIfNotExists(tableName).column("k1", SQLDataType.VARCHAR).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k1")).values("someKey").execute();

		Assertions.assertThatThrownBy(() -> table.streamSlices(qK1).toList()).isInstanceOf(DataAccessException.class);
	}

	@Test
	public void testReturn_nullableamb() {
		dsl.createTableIfNotExists(tableName)
				.column("k1", SQLDataType.DOUBLE)
				.column("k2", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k1")).values(123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k2")).values(234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k1"), DSL.field("k2")).values(345, 456).execute();

		List<Map<String, ?>> tableStream = table.streamSlices(qK1).toList();

		Assertions.assertThat(tableStream).contains(Map.of("k1", BigDecimal.valueOf(0D + 123 + 345))).hasSize(1);
	}

	@Test
	public void testReturn_nullableColumn_filterEquals() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("b", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("b"), DSL.field("k1")).values("b1", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("b"), DSL.field("k1"))
				.values("a2", "b2", 345)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("b"), DSL.field("k1"))
				.values("a1", "b2", 456)
				.execute();

		List<Map<String, ?>> tableStream = table
				.streamSlices(
						TableQuery.edit(qK1).filter(ColumnFilter.builder().column("a").matching("a1").build()).build())
				.toList();

		Assertions.assertThat(tableStream).hasSize(1).contains(Map.of("k1", BigDecimal.valueOf(0D + 123 + 456)));
	}

	@Test
	public void testReturn_nullableColumn_filterIn() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("b", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("b"), DSL.field("k1")).values("b1", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("b"), DSL.field("k1"))
				.values("a2", "b2", 345)
				.execute();

		List<Map<String, ?>> tableStream = table.streamSlices(TableQuery.edit(qK1)
				.filter(ColumnFilter.builder().column("a").matching(Set.of("a1", "a2")).build())
				.build()).toList();

		Assertions.assertThat(tableStream).contains(Map.of("k1", BigDecimal.valueOf(0D + 123 + 345))).hasSize(1);
	}

	@Test
	public void testReturn_nullableColumn_groupBy() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("b", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("b"), DSL.field("k1")).values("b1", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("b"), DSL.field("k1"))
				.values("a2", "b2", 345)
				.execute();

		List<Map<String, ?>> tableStream =
				table.streamSlices(TableQuery.edit(qK1).groupBy(GroupByColumns.named("a")).build()).toList();

		Assertions.assertThat(tableStream)
				.hasSize(3)
				.anySatisfy(
						m -> Assertions.assertThat(m).isEqualTo(Map.of("a", "a1", "k1", BigDecimal.valueOf(0D + 123))))
				.anySatisfy(m -> Assertions.assertThat((Map) m)
						.hasSize(2)
						// TODO We need an option to handle null with a default value
						.containsEntry("a", null)
						.containsEntry("k1", BigDecimal.valueOf(0D + 234)))
				.anySatisfy(
						m -> Assertions.assertThat(m).isEqualTo(Map.of("a", "a2", "k1", BigDecimal.valueOf(0D + 345))));
	}

	@Test
	public void testFieldWithSpace() {
		dsl.createTableIfNotExists(tableName)
				.column("with space", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field(DSL.name("with space")), DSL.field("k1"))
				.values("a1", 123)
				.execute();

		List<Map<String, ?>> tableStream = table.streamSlices(
				TableQuery.edit(qK1).filter(ColumnFilter.builder().column("with space").matching("a1").build()).build())
				.toList();

		Assertions.assertThat(tableStream).hasSize(1).contains(Map.of("k1", BigDecimal.valueOf(0D + 123)));
	}

	@Test
	public void testWholeQuery() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.INTEGER)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 234).execute();

		forest.addMeasure(k1Sum);
		forest.addMeasure(k1SumSquared);

		ITabularView result = wrapInCube(forest).execute(CubeQuery.builder().measure(k1SumSquared.getName()).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1SumSquared.getName(), (long) Math.pow(123 + 234, 2)));
	}

	// https://stackoverflow.com/questions/361747/what-does-the-symbol-do-in-sql
	@Test
	public void testCubeQuery_FilterA1_groupByB_columnWithAtSymbol() {
		dsl.createTableIfNotExists(tableName)
				.column("a@a@a", SQLDataType.VARCHAR)
				.column("b@b@b", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.INTEGER)
				.execute();
		dsl.insertInto(DSL.table(tableName),
				DSL.field(DSL.name("a@a@a")),
				DSL.field(DSL.name("b@b@b")),
				DSL.field("k1")).values("a1", "b1", 123).execute();
		dsl.insertInto(DSL.table(tableName),
				DSL.field(DSL.name("a@a@a")),
				DSL.field(DSL.name("b@b@b")),
				DSL.field("k1")).values("a2", "b2", 234).execute();
		dsl.insertInto(DSL.table(tableName),
				DSL.field(DSL.name("a@a@a")),
				DSL.field(DSL.name("b@b@b")),
				DSL.field("k1")).values("a1", "b2", 345).execute();

		forest.addMeasure(k1Sum);
		forest.addMeasure(k1SumSquared);

		CubeWrapper cube = wrapInCube(forest);

		{
			ITabularView result = cube.execute(CubeQuery.builder()
					.measure(k1Sum.getName())
					.andFilter("a@a@a", "a1")
					.groupByAlso("b@b@b")

					.build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("b@b@b", "b1"), Map.of(k1Sum.getName(), 0L + 123))
					.containsEntry(Map.of("b@b@b", "b2"), Map.of(k1Sum.getName(), 0L + 345));
		}

		{
			ITabularView result = cube.execute(CubeQuery.builder()
					.measure(k1SumSquared.getName())
					.andFilter("a@a@a", "a1")
					.groupByAlso("b@b@b")

					.build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("b@b@b", "b1"), Map.of(k1SumSquared.getName(), (long) Math.pow(123, 2)))
					.containsEntry(Map.of("b@b@b", "b2"), Map.of(k1SumSquared.getName(), (long) Math.pow(345, 2)));
		}
	}

	@Test
	public void testFilterUnknownColumn() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.INTEGER)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();

		forest.addMeasure(k1Sum);

		Assertions.assertThatThrownBy(() -> wrapInCube(forest).execute(
				CubeQuery.builder().measure(k1Sum.getName()).andFilter("unknownColumn", "unknownValue").build()))
				.isInstanceOf(RuntimeException.class)
				.hasStackTraceContaining("Binder Error: Referenced column \"unknownColumn\" not found in FROM clause!")
				.hasRootCauseInstanceOf(SQLException.class);
	}

	@Test
	public void testGroupByUnknownColumn() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.INTEGER)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();

		forest.addMeasure(k1Sum);

		Assertions
				.assertThatThrownBy(() -> wrapInCube(forest)
						.execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso("unknownColumn").build()))
				.isInstanceOf(IllegalArgumentException.class)
				// .hasStackTraceContaining("source=TableQuery")
				.hasStackTraceContaining("Binder Error: Referenced column \"unknownColumn\" not found in FROM clause!");
	}

	@Test
	public void testAggregatorHasDifferentColumnName() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.INTEGER)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 234).execute();

		Aggregator kSumOverk1 =
				Aggregator.builder().name("k").columnName("k1").aggregationKey(SumAggregation.KEY).build();

		forest.addMeasure(kSumOverk1);

		ITabularView result = wrapInCube(forest).execute(CubeQuery.builder().measure(kSumOverk1.getName()).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(kSumOverk1.getName(), 0L + 123 + 234));
	}

	@Test
	public void testLikeColumnMatcher() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a2", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a12", 345).execute();

		forest.addMeasure(k1Sum);

		ITabularView result = wrapInCube(forest).execute(CubeQuery.builder()
				.measure(k1Sum.getName())
				.andFilter("a", LikeMatcher.builder().like("a1%").build())
				.build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 123 + 345));
	}

	@Test
	public void testFilterNone() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a2", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a12", 345).execute();

		forest.addMeasure(k1Sum);

		ITabularView result = wrapInCube(forest)
				.execute(CubeQuery.builder().measure(k1Sum.getName()).filter(IAdhocFilter.MATCH_NONE).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).isEmpty();
	}

	@Test
	public void testUnknownColumn_filter() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();

		forest.addMeasure(k1Sum);

		Assertions.assertThatThrownBy(() -> {
			wrapInCube(forest).execute(
					CubeQuery.builder().measure(k1Sum.getName()).andFilter("unknownColumn", "someValue").build());
		})
				.isInstanceOf(IllegalArgumentException.class)
				// .hasStackTraceContaining("source=TableQuery")
				.hasRootCauseInstanceOf(SQLException.class)
				.hasStackTraceContaining("Binder Error: Referenced column \"unknownColumn\" not found in FROM clause");
	}

	@Test
	public void testCubeQuery_sumFilterGroupByk1() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();

		forest.addMeasure(k1Sum);

		{
			CubeQuery query = CubeQuery.builder().measure(k1Sum.getName()).groupByAlso("k1").build();

			ITabularView result = wrapInCube(forest).execute(query);
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.hasSize(1)
					.containsEntry(Map.of("k1", 0D + 123), Map.of("k1", 0L + 123));
		}
	}

	@Test
	public void testCubeQuery_countAsterisk() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a2", 234).execute();

		forest.addMeasure(countAsterisk);

		{
			CubeQuery query = CubeQuery.builder().measure(countAsterisk).explain(true).build();

			ITabularView result = wrapInCube(forest).execute(query);
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.hasSize(1)
					.containsEntry(Map.of(), Map.of(countAsterisk.getName(), 0L + 2));
		}
	}

	@Test
	public void testDistinct() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a2", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 345).execute();

		forest.addMeasure(k1Sum);

		// groupBy `a` with no measure: this is a distinct query on given groupBy
		ITabularView result = wrapInCube(forest).execute(CubeQuery.builder().groupByAlso("a").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("a", "a1"), Map.of())
				.containsEntry(Map.of("a", "a2"), Map.of())
				.hasSize(2);
	}

	@Test
	public void testEmpty() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a2", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 345).execute();

		forest.addMeasure(k1Sum);

		// groupBy `a` with no measure: this is a distinct query on given groupBy
		ITabularView result =
				wrapInCube(forest).execute(CubeQuery.builder().groupByAlso("a").measure(Aggregator.empty()).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("a", "a1"), Map.of())
				.containsEntry(Map.of("a", "a2"), Map.of())
				.hasSize(2);
	}

	@Test
	public void testCountAsterisk() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a2", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 345).execute();

		forest.addMeasure(k1Sum);

		// groupBy `a` with no measure: this is a distinct query on given groupBy
		ITabularView result = wrapInCube(forest)
				.execute(CubeQuery.builder().groupByAlso("a").measure(Aggregator.countAsterisk()).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("a", "a1"), Map.of("count(*)", 2L))
				.containsEntry(Map.of("a", "a2"), Map.of("count(*)", 1L))
				.hasSize(2);
	}

	@Test
	public void testDescribe() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a2", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 345).execute();

		forest.addMeasure(k1Sum);

		CubeWrapper cube = wrapInCube(forest);

		Assertions.assertThat(cube.getColumnTypes())
				.containsEntry("a", String.class)
				.containsEntry("k1", Double.class)
				.hasSize(2);
	}

	@Test
	public void testFilterOnAggregates_aggregateNameIsAlsoColumnName() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a2", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 345).execute();

		forest.addMeasure(k1Sum);

		ITabularView result = wrapInCube(forest).execute(CubeQuery.builder()
				.groupByAlso("a")
				.measure(k1Sum)
				.andFilter(k1Sum.getName(),
						ComparingMatcher.builder()
								.greaterThan(true)
								.matchIfEqual(false)
								.matchIfNull(false)
								// This will filter `a2=234`
								.operand(400)
								.build())
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		// `k1` is ambiguous as it is both a column and a measure name
		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(0);
	}

	@Test
	public void testFilterOnAggregates() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a2", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 345).execute();

		Aggregator k1Sum =
				Aggregator.builder().name("k1_SUM").aggregationKey(SumAggregation.KEY).columnName("k1").build();
		forest.addMeasure(k1Sum);

		Assertions.assertThatThrownBy(() -> wrapInCube(forest).execute(CubeQuery.builder()
				.groupByAlso("a")
				.measure(k1Sum)
				.andFilter(k1Sum.getName(),
						ComparingMatcher.builder()
								.greaterThan(true)
								.matchIfEqual(false)
								.matchIfNull(false)
								// This will filter `a2=234`
								.operand(400)
								.build())
				// `WHERE clause cannot contain aggregates!` msg is not easy to get given we actually referred to
				// aggregator by alias, so condition of aggregator name leads to an unknwonName error
				.build())).hasStackTraceContaining("Binder Error: ");
	}

	@Test
	public void testFilterOnLocalDate_asString() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.LOCALDATE)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		LocalDate today = LocalDate.now();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values(today, 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values(today.plusDays(1), 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values(today, 345).execute();

		Aggregator k1Sum =
				Aggregator.builder().name("k1_SUM").aggregationKey(SumAggregation.KEY).columnName("k1").build();
		forest.addMeasure(k1Sum);

		ITabularView result = wrapInCube(forest)
				.execute(CubeQuery.builder().andFilter("a", StringMatcher.hasToString(today)).measure(k1Sum).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 123 + 345))
				.hasSize(1);
	}
}

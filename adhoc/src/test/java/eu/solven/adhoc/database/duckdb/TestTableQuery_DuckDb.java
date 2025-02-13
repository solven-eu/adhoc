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

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.value.InMatcher;
import org.assertj.core.api.Assertions;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.AdhocCubeWrapper;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.AdhocTestHelper;
import eu.solven.adhoc.measure.AdhocMeasureBag;
import eu.solven.adhoc.measure.step.Aggregator;
import eu.solven.adhoc.measure.sum.SumAggregator;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.storage.MapBasedTabularView;
import eu.solven.adhoc.table.sql.AdhocJooqTableWrapper;
import eu.solven.adhoc.table.sql.AdhocJooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.DuckDbHelper;

public class TestTableQuery_DuckDb implements IAdhocTestConstants {

	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	AdhocQueryEngine aqe = AdhocQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()::post).build();
	AdhocMeasureBag measureBag = AdhocMeasureBag.builder().name("duckdb").build();

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

	@Test
	public void testTableDoesNotExists() {
		Assertions.assertThatThrownBy(() -> jooqDb.openDbStream(qK1).toList()).isInstanceOf(DataAccessException.class);
	}

	@Test
	public void testEmptyDb() {
		dsl.createTableIfNotExists(tableName).column("k1", SQLDataType.DOUBLE).execute();

		List<Map<String, ?>> dbStream = jooqDb.openDbStream(qK1).toList();

		Assertions.assertThat(dbStream).isEmpty();
	}

	@Test
	public void testReturnAll() {
		dsl.createTableIfNotExists(tableName).column("k1", SQLDataType.DOUBLE).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k1")).values(123).execute();

		List<Map<String, ?>> dbStream = jooqDb.openDbStream(qK1).toList();

		Assertions.assertThat(dbStream).hasSize(1).contains(Map.of("k1", BigDecimal.valueOf(0D + 123)));
	}

	@Test
	public void test_sumOverVarChar() {
		dsl.createTableIfNotExists(tableName).column("k1", SQLDataType.VARCHAR).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k1")).values("someKey").execute();

		Assertions.assertThatThrownBy(() -> jooqDb.openDbStream(qK1).toList()).isInstanceOf(DataAccessException.class);
	}

	@Test
	public void testReturn_nullableMeasures() {
		dsl.createTableIfNotExists(tableName)
				.column("k1", SQLDataType.DOUBLE)
				.column("k2", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k1")).values(123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k2")).values(234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k1"), DSL.field("k2")).values(345, 456).execute();

		List<Map<String, ?>> dbStream = jooqDb.openDbStream(qK1).toList();

		Assertions.assertThat(dbStream).contains(Map.of("k1", BigDecimal.valueOf(0D + 123 + 345))).hasSize(1);
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

		List<Map<String, ?>> dbStream = jooqDb.openDbStream(TableQuery.edit(qK1)
				.filter(ColumnFilter.builder().column("a").matching("a1").build())
				.explain(true)
				.build()).toList();

		Assertions.assertThat(dbStream).hasSize(1).contains(Map.of("k1", BigDecimal.valueOf(0D + 123 + 456)));
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

		List<Map<String, ?>> dbStream = jooqDb.openDbStream(TableQuery.edit(qK1)
				.filter(ColumnFilter.builder().column("a").matching(Set.of("a1", "a2")).build())
				.explain(true)
				.build()).toList();

		Assertions.assertThat(dbStream).contains(Map.of("k1", BigDecimal.valueOf(0D + 123 + 345))).hasSize(1);
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

		List<Map<String, ?>> dbStream =
				jooqDb.openDbStream(TableQuery.edit(qK1).groupBy(GroupByColumns.named("a")).build()).toList();

		Assertions.assertThat(dbStream)
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

		List<Map<String, ?>> dbStream = jooqDb.openDbStream(
				TableQuery.edit(qK1).filter(ColumnFilter.builder().column("with space").matching("a1").build()).build())
				.toList();

		Assertions.assertThat(dbStream).hasSize(1).contains(Map.of("k1", BigDecimal.valueOf(0D + 123)));
	}

	@Test
	public void testWholeQuery() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.INTEGER)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 234).execute();

		measureBag.addMeasure(k1Sum);
		measureBag.addMeasure(k1SumSquared);

		ITabularView result = wrapInCube(measureBag)
				.execute(AdhocQuery.builder().measure(k1SumSquared.getName()).debug(true).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1SumSquared.getName(), (long) Math.pow(123 + 234, 2)));
	}

	// https://stackoverflow.com/questions/361747/what-does-the-symbol-do-in-sql
	@Test
	public void testAdhocQuery_FilterA1_groupByB_columnWithAtSymbol() {
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

		measureBag.addMeasure(k1Sum);
		measureBag.addMeasure(k1SumSquared);

		AdhocCubeWrapper cube = wrapInCube(measureBag);

		{
			ITabularView result = cube.execute(AdhocQuery.builder()
					.measure(k1Sum.getName())
					.andFilter("a@a@a", "a1")
					.groupByAlso("b@b@b")
					.debug(true)
					.build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("b@b@b", "b1"), Map.of(k1Sum.getName(), 0L + 123))
					.containsEntry(Map.of("b@b@b", "b2"), Map.of(k1Sum.getName(), 0L + 345));
		}

		{
			ITabularView result = cube.execute(AdhocQuery.builder()
					.measure(k1SumSquared.getName())
					.andFilter("a@a@a", "a1")
					.groupByAlso("b@b@b")
					.debug(true)
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

		measureBag.addMeasure(k1Sum);

		Assertions
				.assertThatThrownBy(() -> wrapInCube(measureBag).execute(
						AdhocQuery.builder().measure(k1Sum.getName()).andFilter("b", "a1").debug(true).build()))
				.isInstanceOf(RuntimeException.class)
				.hasCauseInstanceOf(DataAccessException.class)
				.hasRootCauseInstanceOf(SQLException.class);
	}

	@Test
	public void testGroupByUnknownColumn() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.INTEGER)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();

		measureBag.addMeasure(k1Sum);

		Assertions
				.assertThatThrownBy(() -> wrapInCube(measureBag)
						.execute(AdhocQuery.builder().measure(k1Sum.getName()).groupByAlso("b").debug(true).build()))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("from source=TableQuery")
				.hasCauseInstanceOf(DataAccessException.class);
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
				Aggregator.builder().name("k").columnName("k1").aggregationKey(SumAggregator.KEY).build();

		measureBag.addMeasure(kSumOverk1);

		ITabularView result =
				wrapInCube(measureBag).execute(AdhocQuery.builder().measure(kSumOverk1.getName()).debug(true).build());
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

		measureBag.addMeasure(k1Sum);

		ITabularView result = wrapInCube(measureBag).execute(AdhocQuery.builder()
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

		measureBag.addMeasure(k1Sum);

		ITabularView result = wrapInCube(measureBag).execute(AdhocQuery.builder()
				.measure(k1Sum.getName())
				.filter(IAdhocFilter.MATCH_NONE)
				.build());
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

		measureBag.addMeasure(k1Sum);

		Assertions.assertThatThrownBy(() -> {
			wrapInCube(measureBag).execute(AdhocQuery.builder()
					.measure(k1Sum.getName())
					// .groupByAlso("unknownColumn")
					.andFilter("unknownColumn", "someValue")
					.debug(true)
					.build());
		})
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("from source=TableQuery")
				.hasCauseInstanceOf(DataAccessException.class)
				.hasMessageContaining("from source=TableQuery")
				.hasRootCauseInstanceOf(SQLException.class)
				.hasStackTraceContaining("Binder Error: Referenced column \"unknownColumn\" not found in FROM clause");
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

		measureBag.addMeasure(k1Sum);

		// groupBy `a` with no measure: this is a distinct query on given groupBy
		ITabularView result = wrapInCube(measureBag).execute(AdhocQuery.builder().groupByAlso("a").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("a", "a1"), Map.of("COUNT_ASTERISK", 2L))
				.containsEntry(Map.of("a", "a2"), Map.of("COUNT_ASTERISK", 1L));
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

		measureBag.addMeasure(k1Sum);

		AdhocCubeWrapper cube = wrapInCube(measureBag);

		Assertions.assertThat(cube.getColumns())
				.containsEntry("a", String.class)
				.containsEntry("k1", Double.class)
				.hasSize(2);
	}
}

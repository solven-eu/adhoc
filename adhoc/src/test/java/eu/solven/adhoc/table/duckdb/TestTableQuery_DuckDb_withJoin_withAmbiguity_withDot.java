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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.cube.CubeWrapper.CubeWrapperBuilder;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.transcoder.MapTableTranscoder;

public class TestTableQuery_DuckDb_withJoin_withAmbiguity_withDot extends ADuckDbJooqTest
		implements IAdhocTestConstants {

	String factTable = "someFactTable";
	String joinedTable = "someJoinedName";
	String joinedAlias = "p";

	Table<Record> fromClause = DSL.table(DSL.name(factTable))
			.as("f")
			.leftJoin(DSL.table(DSL.name(joinedTable)).as(joinedAlias))
			.using(DSL.field("productId"));

	@Override
	public ITableWrapper makeTable() {
		return new JooqTableWrapper(factTable,
				JooqTableWrapperParameters.builder().dslSupplier(dslSupplier).table(fromClause).build());
	}

	TableQuery qK1 = TableQuery.builder().aggregators(Set.of(k1Sum)).build();

	Aggregator k1k2Sum =
			Aggregator.builder().name("k1k2").columnName("\"k1.k2\"").aggregationKey(SumAggregation.KEY).build();

	Aggregator k3Sum =
			Aggregator.builder().name("k3").columnName(joinedAlias + ".k3").aggregationKey(SumAggregation.KEY).build();

	@Test
	public void testTableDoesNotExists() {
		Assertions.assertThatThrownBy(() -> table().streamSlices(qK1).toList())
				.isInstanceOf(DataAccessException.class)
				.hasMessageContaining("Table with name someFactTable does not exist!");
	}

	private void initTables() {
		dsl.createTableIfNotExists(factTable)
				.column("productId", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.column("k1.k2", SQLDataType.DOUBLE)
				.column("name", SQLDataType.VARCHAR)
				// BEWARE This is not a joined column, it (weirdly) looks like a joined column.
				.column(joinedAlias + ".name", SQLDataType.VARCHAR)
				.execute();
		dsl.createTableIfNotExists(joinedTable)
				.column("productId", SQLDataType.VARCHAR)
				.column("productName", SQLDataType.VARCHAR)
				.column("countryId", SQLDataType.VARCHAR)
				.column("name", SQLDataType.VARCHAR)
				.column("k3", SQLDataType.DOUBLE)
				.execute();
	}

	private void insertData() {
		// Carot is fully joined
		dsl.insertInto(DSL.table(factTable),
				DSL.field("k1"),
				DSL.field(DSL.quotedName("k1.k2")),
				DSL.field("productId"),
				DSL.field("name"),
				DSL.field(DSL.quotedName(joinedAlias + ".name")))
				.values(123, 234, "carotId", "carot", "carotLegacy")
				.execute();
		dsl.insertInto(DSL.table(joinedTable),
				DSL.field("productId"),
				DSL.field("productName"),
				DSL.field("countryId"),
				DSL.field("name"),
				DSL.field("k3")).values("carotId", "Carotte", "FRA", "carotNew", 345).execute();
	}

	@Override
	public CubeWrapperBuilder makeCube() {
		return super.makeCube().columnsManager(ColumnsManager.builder()
				.transcoder(MapTableTranscoder.builder().queriedToUnderlying("name", joinedAlias + ".name").build())
				.build());
	}

	@BeforeEach
	public void registerMeasures() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(k1k2Sum);
		forest.addMeasure(k3Sum);
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
				.containsEntry("productId", String.class)
				.containsEntry("productName", String.class)
				// `name` is very ambiguous, as it is provided by the base table and 2 JOINed tables
				.containsEntry("name", String.class)
				// BEWARE This is not a joined column, it (weirdly) looks like a joined column.
				.containsEntry("p.name", String.class)
				.containsEntry("k1", Double.class)
				.containsEntry("k1.k2", Double.class)
				.containsEntry("k3", Double.class)
				.hasSize(8);
	}

	@Test
	public void testDescribe() {
		initTables();
		insertData();

		ICubeWrapper cube = cube();
		cube.getColumnTypes().keySet().forEach(column -> {
			if (Set.of("k1.k2").contains(column)) {
				Assertions.assertThatThrownBy(() -> {
					CoordinatesSample sample = cube.getCoordinates(column, IValueMatcher.MATCH_ALL, 10);
					sample.getCoordinates();
				}).isInstanceOf(DataAccessException.class);
			} else {
				CoordinatesSample sample = cube.getCoordinates(column, IValueMatcher.MATCH_ALL, 10);
				Assertions.assertThat(sample.getCoordinates()).describedAs("c=%s", column).isNotEmpty();

				if ("p.name".equals(column)) {
					// TODO We expected carotLegacy, from baseTabe
					Assertions.assertThat(sample.getCoordinates()).contains("carotNew");
				}
			}
		});
	}

	@Test
	public void testGrandTotal() {
		initTables();
		insertData();

		ITabularView result = cube()
				.execute(CubeQuery.builder().measure(k1Sum.getName(), k1k2Sum.getName(), k3Sum.getName()).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasEntrySatisfying(Map.of(), m -> {
			Assertions.assertThat(m)
					.isEqualTo(ImmutableMap.builder()
							.put(k1Sum.getName(), 0D + 123)
							.put(k1k2Sum.getName(), 0D + 234)
							.put(k3Sum.getName(), 0D + 345)
							.build());
		});
	}

	@Test
	public void testGroupBy() {
		initTables();
		insertData();

		cube().getColumnTypes().keySet().forEach(column -> {
			if (Set.of("k1.k2").contains(column)) {
				Assertions.assertThatThrownBy(
						() -> cube().execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso(column).build()))
						.isInstanceOf(IllegalArgumentException.class);
			} else {
				ITabularView result =
						cube().execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso(column).build());
				MapBasedTabularView mapBased = MapBasedTabularView.load(result);
				Assertions.assertThat(mapBased.getCoordinatesToValues()).isNotEmpty();
			}
		});
	}

}

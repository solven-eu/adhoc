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

import java.sql.Connection;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.column.AdhocColumnsManager;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.ExpressionAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.cube.AdhocQuery;
import eu.solven.adhoc.table.sql.DSLSupplier;
import eu.solven.adhoc.table.sql.DuckDbHelper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.transcoder.IAdhocTableTranscoder;
import eu.solven.adhoc.table.transcoder.MapTableTranscoder;

/**
 * This test complex transcoding scenarios, like one underlying column being mapped multiple times by different queried
 * columns
 */
public class TestTableQuery_DuckDb_Transcoding extends ADuckDbJooqTest implements IAdhocTestConstants {

	String tableName = "someTableName";

	Connection dbConn = DuckDbHelper.makeFreshInMemoryDb();
	DSLSupplier dslSupplier = DSLSupplier.fromConnection(() -> dbConn);

	{
		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);
	}

	private CubeWrapper makecube(IAdhocTableTranscoder transcoder) {
		JooqTableWrapper table = new JooqTableWrapper(tableName,
				JooqTableWrapperParameters.builder().dslSupplier(dslSupplier).tableName(tableName).build());

		CubeWrapper cubeWrapper = CubeWrapper.builder()
				.engine(aqe)
				.table(table)
				.forest(forest)
				.columnsManager(AdhocColumnsManager.builder().transcoder(transcoder).build())
				.build();

		return cubeWrapper;
	}

	@Test
	public void testDifferentQueriedSameUnderlying() {
		// Let's say k1 and k2 rely on the single k DB column
		IAdhocTableTranscoder transcoder =
				MapTableTranscoder.builder().queriedToUnderlying("k1", "k").queriedToUnderlying("k2", "k").build();

		CubeWrapper cube = makecube(transcoder);
		DSLContext dsl = dslSupplier.getDSLContext();

		dsl.createTableIfNotExists(tableName).column("k", SQLDataType.DOUBLE).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k")).values(123).execute();

		{
			ITabularView view = cube.execute(AdhocQuery.builder().measure(k1Sum.getName()).build());

			Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
					.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 123));
		}

		{
			ITabularView view = cube.execute(AdhocQuery.builder().measure(k1Sum.getName(), k2Sum.getName()).build());

			Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
					.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 123, k2Sum.getName(), 0L + 123));
		}
	}

	// We request both a mapped column and directly the underlying column
	@Test
	public void testOverlap() {
		// Let's say k1 and k2 rely on the single k DB column
		IAdhocTableTranscoder transcoder = MapTableTranscoder.builder().queriedToUnderlying("k1", "k").build();

		CubeWrapper cube = makecube(transcoder);
		DSLContext dsl = dslSupplier.getDSLContext();

		dsl.createTableIfNotExists(tableName).column("k", SQLDataType.DOUBLE).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k")).values(123).execute();

		{
			// There is an aggregator which name is also an underlying column: it does not be reverseTranscoded
			Aggregator kSum = Aggregator.builder().name("k").aggregationKey(SumAggregation.KEY).build();
			forest.addMeasure(kSum);

			// We request both k1 and k, which are the same in DB
			ITabularView view = cube.execute(AdhocQuery.builder().measure(k1Sum.getName(), kSum.getName()).build());

			Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
					.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 123, kSum.getName(), 0L + 123));
		}
	}

	@Test
	public void testCycle_measure() {
		// Cycle of length 4: k1 -> k2, k2 -> k3, k3 -> k4, k4 -> k1
		IAdhocTableTranscoder transcoder = MapTableTranscoder.builder()
				.queriedToUnderlying("k1", "k2")
				.queriedToUnderlying("k2", "k3")
				.queriedToUnderlying("k3", "k4")
				.queriedToUnderlying("k4", "k1")
				.build();

		CubeWrapper cube = makecube(transcoder);
		DSLContext dsl = dslSupplier.getDSLContext();

		dsl.createTableIfNotExists(tableName)
				.column("k1", SQLDataType.DOUBLE)
				.column("k2", SQLDataType.DOUBLE)
				.column("k3", SQLDataType.DOUBLE)
				.column("k4", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k1"), DSL.field("k2"), DSL.field("k3"), DSL.field("k4"))
				.values(123, 234, 345, 456)
				.execute();

		{
			ITabularView view = cube.execute(AdhocQuery.builder().measure(k1Sum.getName()).build());

			Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
					.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 234));
		}

		{
			ITabularView view = cube.execute(AdhocQuery.builder().measure(k1Sum.getName(), k2Sum.getName()).build());

			Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
					.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 234, k2Sum.getName(), 0L + 345));
		}

		{
			Aggregator k3Sum = Aggregator.builder().name("k3").aggregationKey(SumAggregation.KEY).build();
			Aggregator k4Sum = Aggregator.builder().name("k4").aggregationKey(SumAggregation.KEY).build();
			forest.addMeasure(k3Sum).addMeasure(k4Sum);

			ITabularView view = cube.execute(AdhocQuery.builder()
					.measure(k1Sum.getName(), k2Sum.getName(), k3Sum.getName(), k4Sum.getName())
					.build());

			Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
					.containsEntry(Map.of(),
							Map.of(k1Sum.getName(),
									0L + 234,
									k2Sum.getName(),
									0L + 345,
									k3Sum.getName(),
									0L + 456,
									k4Sum.getName(),
									0L + 123));
		}
	}

	@Test
	public void testCycle_groupBy() {
		// Cycle of length 4: k1 -> k2, k2 -> k3, k3 -> k4, k4 -> k1
		IAdhocTableTranscoder transcoder = MapTableTranscoder.builder()
				.queriedToUnderlying("k1", "k2")
				.queriedToUnderlying("k2", "k3")
				.queriedToUnderlying("k3", "k4")
				.queriedToUnderlying("k4", "k1")
				.build();

		CubeWrapper cube = makecube(transcoder);
		DSLContext dsl = dslSupplier.getDSLContext();

		dsl.createTableIfNotExists(tableName)
				.column("k1", SQLDataType.DOUBLE)
				.column("k2", SQLDataType.DOUBLE)
				.column("k3", SQLDataType.DOUBLE)
				.column("k4", SQLDataType.DOUBLE)
				.column("k5", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL
				.table(tableName), DSL.field("k1"), DSL.field("k2"), DSL.field("k3"), DSL.field("k4"), DSL.field("k5"))
				.values(123, 234, 345, 456, 567)
				.execute();

		{
			Aggregator k5Sum = Aggregator.builder().name("k5").aggregationKey(SumAggregation.KEY).build();
			forest.addMeasure(k5Sum);

			ITabularView view = cube
					.execute(AdhocQuery.builder().measure(k5Sum.getName()).groupByAlso("k1", "k2", "k3", "k4").build());

			Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
					.containsEntry(Map.of("k1", 234D, "k2", 345D, "k3", 456D, "k4", 123D),
							Map.of(k5Sum.getName(), 0L + 567));
		}
	}

	@Test
	public void testAdhocQuery() {
		// Let's say k1 and k2 rely on the single k DB column
		IAdhocTableTranscoder transcoder = MapTableTranscoder.builder().queriedToUnderlying("k1", "k").build();

		CubeWrapper cube = makecube(transcoder);
		DSLContext dsl = dslSupplier.getDSLContext();

		dsl.createTableIfNotExists(tableName).column("k", SQLDataType.DOUBLE).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k")).values(123).execute();

		{
			AdhocQuery query = AdhocQuery.builder().measure(k1Sum.getName()).andFilter("k1", 123).build();

			forest.addMeasure(k1Sum);

			ITabularView result = cube.execute(query);
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.hasSize(1)
					.containsEntry(Map.of(), Map.of("k1", 0L + 123));
		}
	}

	// https://github.com/duckdb/duckdb/issues/16097
	// https://github.com/jOOQ/jOOQ/issues/17980
	// This is not testing the initial issue since transcoding is moved from `table` to `engine`, as `table` now have
	// less uses of ALIASes.
	@Test
	public void testAdhocQuery_aliasWithNameAlreadyInTable() {
		// Let's say k1 and k2 rely on the single k DB column
		IAdhocTableTranscoder transcoder =
				MapTableTranscoder.builder().queriedToUnderlying("k1", "k").queriedToUnderlying("k2", "k").build();

		CubeWrapper cube = makecube(transcoder);
		DSLContext dsl = dslSupplier.getDSLContext();

		dsl.createTableIfNotExists(tableName)
				.column("k", SQLDataType.DOUBLE)
				.column("k1", SQLDataType.DOUBLE)
				.column("k2", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k"), DSL.field("k1"), DSL.field("k2"))
				.values(123, 234, 345)
				.execute();

		{
			AdhocQuery query =
					AdhocQuery.builder().measure(k1Sum.getName()).andFilter("k1", 123).groupByAlso("k2").build();

			forest.addMeasure(k1Sum);

			ITabularView result = cube.execute(query);
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.hasSize(1)
					// TODO It is unclear why we got Doubles or Longs
					.containsEntry(Map.of("k2", 123.0D), Map.of("k1", 0L + 123));
		}
	}

	@Test
	public void testAdhocQuery_sumFilterGroupByk1() {
		// Let's say k1 and k2 rely on the single k DB column
		IAdhocTableTranscoder transcoder = MapTableTranscoder.builder().queriedToUnderlying("k1", "k").build();

		CubeWrapper cube = makecube(transcoder);
		DSLContext dsl = dslSupplier.getDSLContext();

		dsl.createTableIfNotExists(tableName).column("k", SQLDataType.DOUBLE).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k")).values(123).execute();

		{
			AdhocQuery query =
					AdhocQuery.builder().measure(k1Sum.getName()).andFilter("k1", 123).groupByAlso("k1").build();

			ITabularView result = cube.execute(query);
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.hasSize(1)
					.containsEntry(Map.of("k1", 0D + 123), Map.of("k1", 0L + 123));
		}
	}

	@Test
	public void testAdhocQuery_transcodeAggregatedToExpression() {
		// In this useCase, we rely on a simple FILTER expression
		// While this could be done with a Filtrator, it demonstrate useCases like: potentially more complex
		// expressions, or Atoti ColumnCalculator
		IAdhocTableTranscoder transcoder = MapTableTranscoder.builder()
				.queriedToUnderlying("v_RED", "max(\"v\") FILTER(\"color\" in ('red'))")
				.build();

		CubeWrapper cube = makecube(transcoder);
		DSLContext dsl = dslSupplier.getDSLContext();

		dsl.createTableIfNotExists(tableName)
				.column("v", SQLDataType.DOUBLE)
				.column("color", SQLDataType.VARCHAR)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("v"), DSL.field("color"))
				.values(123, "red")
				.values(234, "blue")
				.values(345, "red")
				.execute();

		Aggregator vRedSum = Aggregator.builder().name("v_RED").aggregationKey(ExpressionAggregation.KEY).build();
		forest.addMeasure(vRedSum);

		{
			AdhocQuery query = AdhocQuery.builder().measure("v_RED").build();

			ITabularView result = cube.execute(query);
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.hasSize(1)
					.containsEntry(Map.of(), Map.of("v_RED", 0D + 345));
		}
	}
}

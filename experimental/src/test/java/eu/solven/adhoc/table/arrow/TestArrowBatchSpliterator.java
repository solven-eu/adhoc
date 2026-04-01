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
package eu.solven.adhoc.table.arrow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;

import org.assertj.core.api.Assertions;
import org.duckdb.DuckDBResultSet;
import org.jooq.ConnectionProvider;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.map.factory.RowSliceFactory;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.duckdb.ADuckDbJooqTest;
import eu.solven.adhoc.table.sql.AggregatedRecordFields;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTabularRecordFactory;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;
import eu.solven.adhoc.table.sql.duckdb.DuckDBTableWrapper;
import eu.solven.adhoc.table.sql.duckdb.DuckDBTableWrapperParameters;

/**
 * White-box tests for {@link ArrowBatchSpliterator} and {@link ArrowFixedBatchSpliterator} splitting mechanics.
 *
 * <p>
 * These tests live in the same package as the production classes so that they can access package-level members (e.g.
 * {@link ArrowFixedBatchSpliterator#MIN_SPLIT_ROWS}).
 *
 * @author Benoit Lacelle
 */
public class TestArrowBatchSpliterator extends ADuckDbJooqTest {

	static final String TABLE = "split_test";

	@Override
	public ITableWrapper makeTable() {

		return new JooqTableWrapper(TABLE, DuckDBHelper.parametersBuilder(dslSupplier).tableName(TABLE).build());
	}

	@BeforeEach
	void setUp() {
		dsl.createTableIfNotExists(TABLE).column("v", SQLDataType.DOUBLE).execute();
		for (int i = 1; i <= 4; i++) {
			dsl.insertInto(DSL.table(TABLE), DSL.field("v")).values((double) i).execute();
		}
	}

	/**
	 * Builds a {@link JooqTabularRecordFactory} that maps the first (and only) Arrow vector to an aggregate named
	 * {@code "v"}.
	 */
	JooqTabularRecordFactory makeFactory() {
		return JooqTabularRecordFactory.builder()
				.globalGroupBy(IGroupBy.GRAND_TOTAL)
				.fields(AggregatedRecordFields.builder().aggregate("v").build())
				.sliceFactory(RowSliceFactory.builder().build())
				.build();
	}

	/**
	 * Builds a {@link QueryPod} for the given {@code minSplitRows} threshold.
	 */
	private QueryPod makeQueryPod(int minSplitRows) {
		DuckDBTableWrapper wrapper = new DuckDBTableWrapper(TABLE,
				DuckDBTableWrapperParameters.builder()
						.base(DuckDBHelper.parametersBuilder(dslSupplier).tableName(TABLE).build())
						.minSplitRows(minSplitRows)
						.build());
		return QueryPod.forTable(wrapper);
	}

	/**
	 * Opens a real {@code arrowReader} backed by the {@code SELECT v FROM TABLE} query, batch-size capped to
	 * {@code batchSize}.
	 *
	 * <p>
	 * The returned {@link AutoCloseable} wraps all JDBC/Arrow resources in reverse-close order.
	 */
	private static final class ArrowHandle implements AutoCloseable {
		final Object arrowReader;
		final Object allocator;
		final ResultSet rs;
		final PreparedStatement stmt;
		final Connection conn;
		final ConnectionProvider cp;

		ArrowHandle(ConnectionProvider cp, String sql, long batchSize) throws Exception {
			this.cp = cp;
			this.conn = cp.acquire();
			this.stmt = conn.prepareStatement(sql);
			this.rs = stmt.executeQuery();
			DuckDBResultSet duckRs = rs.unwrap(DuckDBResultSet.class);
			this.allocator = ArrowReflection.createAllocator();
			this.arrowReader = duckRs.arrowExportStream(allocator, batchSize);
		}

		@Override
		public void close() throws Exception {
			((AutoCloseable) arrowReader).close();
			((AutoCloseable) allocator).close();
			rs.close();
			stmt.close();
			cp.release(conn);
		}
	}

	/**
	 * Collects all {@link ITabularRecord#getAggregate(String) aggregate("v")} values from a spliterator.
	 */
	private static List<Double> drain(Spliterator<ITabularRecord> spl) {
		List<Double> result = new ArrayList<>();
		spl.forEachRemaining(r -> result.add((Double) r.getAggregate("v")));
		return result;
	}

	@Test
	void trySplit_returnsFixedBatchSpliterator_whenEnoughRows() throws Exception {
		// 4 rows, minSplitRows=2 → 4 >= 2*2 → splitting allowed
		try (ArrowHandle h = new ArrowHandle(dsl.configuration().connectionProvider(), "SELECT v FROM " + TABLE, 4L)) {
			ArrowBatchSpliterator spliterator = new ArrowBatchSpliterator(h.arrowReader,
					new ArrowBatchContext(makeFactory(), makeQueryPod(2), 2),
					2);

			// trySplit() loads the first batch internally, then splits it
			Spliterator<ITabularRecord> half = spliterator.trySplit();

			Assertions.assertThat(half)
					.as("splitting a 4-row batch with minSplitRows=2 must succeed")
					.isNotNull()
					.isInstanceOf(ArrowFixedBatchSpliterator.class);

			// The fixed-batch half covers exactly splitSize = 4/2 = 2 rows
			Assertions.assertThat(half.estimateSize()).as("split half must know its exact row count").isEqualTo(2L);

			// Consume both halves and verify no row is lost or duplicated
			List<Double> fromHalf = drain(half);
			List<Double> fromOriginal = drain(spliterator);

			Assertions.assertThat(fromHalf.size() + fromOriginal.size())
					.as("total rows across both halves must equal inserted row count")
					.isEqualTo(4);

			// No value appears in both halves
			fromHalf.forEach(v -> Assertions.assertThat(fromOriginal)
					.as("row value {} must not be duplicated", v)
					.doesNotContain(v));
		}
	}

	@Test
	void trySplit_returnsNull_whenBatchTooSmall() throws Exception {
		// 4 rows, minSplitRows=5 → 4 < 5 → splitting refused
		try (ArrowHandle h = new ArrowHandle(dsl.configuration().connectionProvider(), "SELECT v FROM " + TABLE, 4L)) {
			ArrowBatchSpliterator spliterator = new ArrowBatchSpliterator(h.arrowReader,
					new ArrowBatchContext(makeFactory(), makeQueryPod(5), 5),
					2);

			Spliterator<ITabularRecord> half = spliterator.trySplit();

			Assertions.assertThat(half).as("4 rows < 2*minSplitRows(5) → trySplit must return null").isNull();

			// All 4 rows are still reachable in the original spliterator
			Assertions.assertThat(drain(spliterator).size()).isEqualTo(4);
		}
	}

	@Test
	void fixedBatchSpliterator_trySplit_recursivelyDividesSlice() throws Exception {
		// 4 rows, minSplitRows=2 → first split gives an ArrowFixedBatchSpliterator(2 rows)
		// A second trySplit on that fixed spliterator: 2 >= 2 → allowed → gives 1+1
		try (ArrowHandle h = new ArrowHandle(dsl.configuration().connectionProvider(), "SELECT v FROM " + TABLE, 4L)) {
			ArrowBatchSpliterator spliterator = new ArrowBatchSpliterator(h.arrowReader,
					new ArrowBatchContext(makeFactory(), makeQueryPod(2), 2),
					2);

			ArrowFixedBatchSpliterator fixed = (ArrowFixedBatchSpliterator) spliterator.trySplit();
			Assertions.assertThat(fixed).isNotNull();

			// Recursive split of the fixed half (2 rows, minSplitRows=2 → 2 >= 2 → allowed)
			Spliterator<ITabularRecord> subHalf = fixed.trySplit();
			Assertions.assertThat(subHalf)
					.as("recursive split of a 2-row fixed batch with minSplitRows=2 must succeed")
					.isNotNull()
					.isInstanceOf(ArrowFixedBatchSpliterator.class);
			Assertions.assertThat(subHalf.estimateSize()).isEqualTo(1L);
			Assertions.assertThat(fixed.estimateSize()).isEqualTo(1L);

			// Consume all four pieces and verify completeness
			List<Double> all = new ArrayList<>();
			all.addAll(drain(subHalf));
			all.addAll(drain(fixed));
			all.addAll(drain(spliterator));

			Assertions.assertThat(all).as("all 4 rows must be reachable after recursive splitting").hasSize(4);
		}
	}
}

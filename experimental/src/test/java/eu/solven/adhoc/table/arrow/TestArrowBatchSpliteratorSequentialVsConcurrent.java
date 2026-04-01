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
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.assertj.core.api.Assertions;
import org.duckdb.DuckDBResultSet;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
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
 * Tests sequential vs concurrent processing of Arrow batch spliterator with a large dataset (1 million rows).
 *
 * @author Benoit Lacelle
 */
public class TestArrowBatchSpliteratorSequentialVsConcurrent extends ADuckDbJooqTest {

	static final String TABLE = "big_split_test";
	static final long DATASET_SIZE = 1_000_000L;

	@Override
	public ITableWrapper makeTable() {
		return new JooqTableWrapper(TABLE, DuckDBHelper.parametersBuilder(dslSupplier).tableName(TABLE).build());
	}

	@BeforeEach
	void setUp() {
		dsl.createTableIfNotExists(TABLE).column("v", SQLDataType.BIGINT).execute();

		DSLContext create = dsl;
		create.execute("INSERT INTO " + TABLE + " SELECT * FROM range(0, " + DATASET_SIZE + ") AS t(v)");
	}

	JooqTabularRecordFactory makeFactory() {
		return JooqTabularRecordFactory.builder()
				.globalGroupBy(IGroupBy.GRAND_TOTAL)
				.fields(AggregatedRecordFields.builder().aggregate("v").build())
				.sliceFactory(RowSliceFactory.builder().build())
				.build();
	}

	private QueryPod makeQueryPod(int minSplitRows) {
		DuckDBTableWrapper wrapper = new DuckDBTableWrapper(TABLE,
				DuckDBTableWrapperParameters.builder()
						.base(DuckDBHelper.parametersBuilder(dslSupplier).tableName(TABLE).build())
						.minSplitRows(minSplitRows)
						.build());
		return QueryPod.forTable(wrapper);
	}

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

	private List<Long> drainToList(Spliterator<ITabularRecord> spliterator) {
		return StreamSupport.stream(spliterator, false)
				.map(r -> (Long) r.getAggregate("v"))
				.collect(Collectors.toList());
	}

	private List<Long> drainToListParallel(Spliterator<ITabularRecord> spliterator) {
		return StreamSupport.stream(spliterator, false)
				.parallel()
				.map(r -> (Long) r.getAggregate("v"))
				.collect(Collectors.toList());
	}

	private void assertDistinctRange(List<Long> result) {
		Assertions.assertThat(result).hasSize((int) DATASET_SIZE);

		long min = result.stream().min(Long::compareTo).orElseThrow();
		long max = result.stream().max(Long::compareTo).orElseThrow();
		Assertions.assertThat(min).isEqualTo(0L);
		Assertions.assertThat(max).isEqualTo(DATASET_SIZE - 1);

		long distinctCount = result.stream().distinct().count();
		Assertions.assertThat(distinctCount).isEqualTo(DATASET_SIZE);
	}

	@Test
	void testSequential_streamProcessing() throws Exception {
		try (ArrowHandle h =
				new ArrowHandle(dsl.configuration().connectionProvider(), "SELECT v FROM " + TABLE, DATASET_SIZE)) {
			ArrowBatchSpliterator spliterator = new ArrowBatchSpliterator(h.arrowReader,
					new ArrowBatchContext(makeFactory(), makeQueryPod(1000), 1000),
					2);

			List<Long> result = drainToList(spliterator);

			assertDistinctRange(result);
		}
	}

	@Test
	void testParallel_streamProcessing() throws Exception {
		try (ArrowHandle h =
				new ArrowHandle(dsl.configuration().connectionProvider(), "SELECT v FROM " + TABLE, DATASET_SIZE)) {
			ArrowBatchSpliterator spliterator = new ArrowBatchSpliterator(h.arrowReader,
					new ArrowBatchContext(makeFactory(), makeQueryPod(1000), 1000),
					2);

			List<Long> result = drainToListParallel(spliterator);

			assertDistinctRange(result);
		}
	}
}

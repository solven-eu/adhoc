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
package eu.solven.adhoc.table.duckdb.tpch;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.IDSLSupplier;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;
import eu.solven.adhoc.table.sql.join.JooqTableSupplierBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Diagnostic harness answering a single question: when a fine-grained aggregation over DuckDB is slow, which layer owns
 * the time?
 *
 * <p>
 * It runs the same GROUP BY four ways over identical TPC-H data, from the narrowest to the widest stack. Each stage
 * adds exactly one layer, so consecutive differences attribute the cost:
 * </p>
 *
 * <ol>
 * <li><b>A — DuckDB compute</b>: the aggregation wrapped in {@code SELECT count(*) FROM (…)}, so DuckDB performs the
 * full GROUP BY but returns a single row. No result transfer.</li>
 * <li><b>B — plus JDBC transfer</b>: the same aggregation, every row pulled through the JDBC {@link ResultSet} and
 * every column read. {@code B - A} is the cost of moving rows out of DuckDB.</li>
 * <li><b>C — plus the Adhoc record stream</b>: {@link ITableWrapper#streamSlices}, which layers jOOQ {@code Record}
 * creation and {@code ITabularRecordStream} on top. {@code C - B} is that per-row object cost.</li>
 * </ol>
 *
 * <p>
 * The point is not the absolute numbers — they depend on the machine — but the <em>shape</em>: a workload dominated by
 * A calls for table-side work (partitioning, DuckDB tuning, pre-aggregation), while one dominated by {@code C - A}
 * calls for a columnar fetch path, since that portion is pure per-row overhead that Arrow batches would remove.
 * </p>
 *
 * <p>
 * {@code l_orderkey} is the fine-grained key: at sf=0.1 it yields ~150 K output slices from 600 K input rows, which is
 * the "very fine grained aggregation" shape rather than the handful of rows a dashboard groupBy returns.
 * </p>
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class TestDuckDbLatencyBreakdown {

	private static final String TPCH_EXTENSION_REPOSITORY_URL = "http://extensions.duckdb.org";

	/**
	 * The fine-grained groupBy key: ~150 K distinct orders at sf=0.1.
	 */
	private static final String GROUP_BY_COLUMN = "l_orderkey";

	private static final String MEASURE_COLUMN = "l_extendedprice";

	/**
	 * The aggregation under test, as raw SQL. Stages A and B execute exactly this; stage C asks Adhoc to build its own
	 * equivalent, so the comparison stays honest only as long as the two agree on row count — which the test asserts.
	 */
	private static final String AGGREGATION_SQL =
			"SELECT %s, sum(%s) FROM lineitem GROUP BY %s".formatted(GROUP_BY_COLUMN, MEASURE_COLUMN, GROUP_BY_COLUMN);

	/**
	 * Timed repetitions per stage. The first run pays for class-loading and DuckDB warm-up, so only the last is
	 * reported.
	 */
	private static final int NB_RUNS = 3;

	/**
	 * TPC-H scale factor. Kept at 0.1 (600 K lineitem rows) so the test stays fast in CI; raise it from the command
	 * line (e.g. {@code -Dadhoc.tpch.sf=1}) to check that the layer proportions hold at a larger size.
	 */
	private static final String SCALE_FACTOR = System.getProperty("adhoc.tpch.sf", "0.1");

	/**
	 * Aborts if the DuckDB extension repository is unreachable: DuckDB fails silently when the TPC-H extension cannot
	 * be downloaded, which would surface as a cryptic query error rather than a skip.
	 */
	@BeforeAll
	static void checkExtensionRepositoryConnectivity() {
		try {
			HttpURLConnection connection = (HttpURLConnection) new URL(TPCH_EXTENSION_REPOSITORY_URL).openConnection();
			connection.setConnectTimeout(3000);
			connection.setReadTimeout(3000);
			connection.setRequestMethod("HEAD");
			connection.connect();
			connection.disconnect();
		} catch (IOException e) {
			Assumptions.assumeTrue(false,
					"DuckDB extension repository not reachable (%s): %s — skipping"
							.formatted(TPCH_EXTENSION_REPOSITORY_URL, e.getMessage()));
		}
	}

	@Test
	public void latencyBreakdown_fineGrainedGroupBy() throws SQLException {
		IDSLSupplier dslSupplier = DuckDBHelper.inMemoryDSLSupplier();
		DSLContext dslContext = dslSupplier.getDSLContext();
		dslContext.connection(this::loadTpchData);

		long[] duckDbOnly = new long[NB_RUNS];
		long[] throughJdbc = new long[NB_RUNS];
		long[] throughAdhoc = new long[NB_RUNS];

		long[] rowsFromJdbc = { 0 };
		for (int i = 0; i < NB_RUNS; i++) {
			int run = i;
			dslContext.connection(connection -> {
				duckDbOnly[run] = time(() -> countOnly(connection));
				throughJdbc[run] = time(() -> rowsFromJdbc[0] = fetchAllRows(connection));
			});
		}

		ITableWrapper table = makeLineitemTable(dslSupplier);
		long rowsFromAdhoc = 0;
		for (int i = 0; i < NB_RUNS; i++) {
			int run = i;
			long[] rows = { 0 };
			throughAdhoc[run] = time(() -> rows[0] = streamThroughAdhoc(table));
			rowsFromAdhoc = rows[0];
		}

		long a = duckDbOnly[NB_RUNS - 1];
		long b = throughJdbc[NB_RUNS - 1];
		long c = throughAdhoc[NB_RUNS - 1];

		log.info("""

				DuckDB latency breakdown — sf={} — {}
				  {} output slices
				  A. DuckDB compute only (count(*) wrapper) : {}ms
				  B. + JDBC transfer of every row           : {}ms   (transfer = {}ms)
				  C. + Adhoc record stream                  : {}ms   (per-row object cost = {}ms)
				  => DuckDB owns {}% of the end-to-end cost; the fetch path owns {}%""",
				SCALE_FACTOR,
				AGGREGATION_SQL,
				rowsFromJdbc[0],
				a,
				b,
				b - a,
				c,
				c - b,
				percent(a, c),
				percent(c - a, c));

		// Both paths must aggregate the same data, else the comparison is meaningless.
		Assertions.assertThat(rowsFromAdhoc)
				.as("Adhoc and raw JDBC must return the same number of slices")
				.isEqualTo(rowsFromJdbc[0]);
		// TPC-H mandates 1,500,000 orders per unit of scale factor, hence as many distinct l_orderkey.
		Assertions.assertThat(rowsFromJdbc[0])
				.as("fine-grained groupBy must yield one slice per order")
				.isEqualTo((long) (1_500_000 * Double.parseDouble(SCALE_FACTOR)));
	}

	/**
	 * Stage A: DuckDB performs the whole GROUP BY but returns one row, so nothing is transferred.
	 *
	 * @return the number of groups, as counted inside DuckDB
	 */
	protected long countOnly(Connection connection) {
		try (Statement s = connection.createStatement();
				ResultSet rs = s.executeQuery("SELECT count(*) FROM (%s)".formatted(AGGREGATION_SQL))) {
			rs.next();
			return rs.getLong(1);
		} catch (SQLException e) {
			throw new IllegalStateException("Issue counting groups", e);
		}
	}

	/**
	 * Stage B: same aggregation, every row pulled through JDBC. Both columns are read so the driver actually
	 * materialises each value rather than skipping it.
	 *
	 * @return the number of rows read
	 */
	protected long fetchAllRows(Connection connection) {
		long rows = 0;
		try (Statement s = connection.createStatement(); ResultSet rs = s.executeQuery(AGGREGATION_SQL)) {
			while (rs.next()) {
				rs.getLong(1);
				rs.getDouble(2);
				rows++;
			}
		} catch (SQLException e) {
			throw new IllegalStateException("Issue fetching rows", e);
		}
		return rows;
	}

	/**
	 * Stage C: the same aggregation expressed as a {@link TableQueryV4} and consumed through Adhoc's table layer.
	 *
	 * @return the number of slices produced
	 */
	protected long streamThroughAdhoc(ITableWrapper table) {
		FilteredAggregator sumPrice = FilteredAggregator.builder().aggregator(Aggregator.sum(MEASURE_COLUMN)).build();
		TableQueryV4 tableQuery =
				TableQueryV4.builder().groupByToAggregator(GroupByColumns.named(GROUP_BY_COLUMN), sumPrice).build();

		QueryPod queryPod = QueryPod.forTable(table);
		try (ITabularRecordStream stream = table.streamSlices(queryPod, tableQuery)) {
			List<Map<String, ?>> rows = stream.toList();
			return rows.size();
		}
	}

	protected ITableWrapper makeLineitemTable(IDSLSupplier dslSupplier) {
		// No joins: a fine-grained aggregation on lineitem columns alone, so join cost cannot pollute the measurement.
		JooqTableSupplierBuilder lineitem = JooqTableSupplierBuilder.builder()
				.dslSupplier(dslSupplier)
				.baseTable(DSL.table("lineitem"))
				.baseTableAlias("lineitem")
				.build();

		JooqTableWrapperParameters parameters =
				DuckDBHelper.parametersBuilder(dslSupplier).tableSupplier(lineitem.build()).build();

		return new JooqTableWrapper("tpch_lineitem", parameters);
	}

	protected void loadTpchData(Connection connection) throws SQLException {
		try (Statement s = connection.createStatement()) {
			s.execute("INSTALL tpch");
			s.execute("LOAD tpch");
			s.execute("CALL dbgen(sf = %s)".formatted(SCALE_FACTOR));
		}
	}

	protected long time(Runnable runnable) {
		long start = System.currentTimeMillis();
		runnable.run();
		return System.currentTimeMillis() - start;
	}

	protected long percent(long part, long total) {
		if (total == 0) {
			return 0;
		}
		return 100 * part / total;
	}
}

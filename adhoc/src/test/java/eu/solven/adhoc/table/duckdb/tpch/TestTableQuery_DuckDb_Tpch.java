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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.cube.CubeWrapper.CubeWrapperBuilder;
import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.example.tpch.TpchSchema;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.measure.forest.IMeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.ExpressionAggregation;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.duckdb.ADuckDbJooqTest;
import lombok.extern.slf4j.Slf4j;



/**
 * Integration tests for the TPC-H example schema.
 *
 * <p>
 * TPC-H is an industry-standard decision-support benchmark focused on orders and line items. Schema reference:
 * https://docs.snowflake.com/en/user-guide/sample-data-tpch
 *
 * <p>
 * Data is generated via DuckDB's built-in TPC-H extension at scale factor 0.1 (≈ 600 K line items).
 *
 * @author Benoit Lacelle
 * @see TpchSchema
 */
@Slf4j
public class TestTableQuery_DuckDb_Tpch extends ADuckDbJooqTest {

	private static final String TPCH_EXTENSION_REPOSITORY_URL = "http://extensions.duckdb.org";

	/**
	 * Aborts all tests in this class if the DuckDB extension repository is not reachable (e.g. corporate proxy
	 * blocking outbound HTTP). DuckDB silently fails to load the TPC-H extension when the download is blocked, leading
	 * to cryptic query failures rather than a clear skip.
	 */
	@BeforeAll
	static void checkExtensionRepositoryConnectivity() {
		try {
			HttpURLConnection connection = (HttpURLConnection) new URL(TPCH_EXTENSION_REPOSITORY_URL).openConnection();
			connection.setConnectTimeout(3_000);
			connection.setReadTimeout(3_000);
			connection.setRequestMethod("HEAD");
			connection.connect();
			connection.disconnect();
		} catch (IOException e) {
			Assumptions.assumeTrue(false,
					"DuckDB extension repository not reachable (%s): %s — skipping TPC-H tests"
							.formatted(TPCH_EXTENSION_REPOSITORY_URL, e.getMessage()));
		}
	}

	TpchSchema tpchSchema = new TpchSchema();

	IMeasureForest forest = tpchSchema.getForest(tpchSchema.getName());

	@Override
	public ITableWrapper makeTable() {
		return tpchSchema.getTable(tpchSchema.getName());
	}

	@Override
	public CubeWrapperBuilder makeCube() {
		return tpchSchema
				.makeCube(AdhocSchema.builder().engine(engine()).env(env).build(), tpchSchema, table(), forest);
	}

	/**
	 * Grand-total row count: for sf=0.1 the TPC-H spec mandates exactly 600,572 lineitem rows.
	 */
	@Test
	public void testGrandTotal_count() {
		ITabularView result = cube().execute(CubeQuery.builder().measure("count(*)").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v).containsEntry("count(*)", 600_572L);
		});
	}

	/**
	 * Grand-total revenue: sum of {@code l_extendedprice * (1 - l_discount)} across all line items. Verifies that the
	 * {@code ExpressionAggregation} SQL expression is pushed down correctly.
	 */
	@Test
	public void testGrandTotal_revenue() {
		ITabularView result = cube().execute(CubeQuery.builder().measure("revenue").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v).hasEntrySatisfying("revenue", revenue -> {
				// Revenue for sf=0.1 is approximately 20.5 billion (sum of extendedprice*(1-discount))
				Assertions.assertThat(revenue).asInstanceOf(InstanceOfAssertFactories.DOUBLE).isBetween(2.0e10, 2.1e10);
			});
		});
	}

	/**
	 * Grand-total order count: number of distinct orders. For sf=0.1 the TPC-H spec mandates exactly 150,000 orders.
	 */
	@Test
	public void testGrandTotal_orderCount() {
		ITabularView result = cube().execute(CubeQuery.builder().measure("order_count").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v).containsEntry("order_count", 150_000L);
		});
	}

	/**
	 * Grand-total customer count: number of distinct customers who placed at least one order. For sf=0.1, all 15,000
	 * customers have orders in the standard TPC-H dataset.
	 */
	@Test
	public void testGrandTotal_customerCount() {
		ITabularView result = cube().execute(CubeQuery.builder().measure("customer_count").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v).hasEntrySatisfying("customer_count", count -> {
				// Not all 15,000 customers place orders; for sf=0.1 roughly 10,000 do
				Assertions.assertThat(count).asInstanceOf(InstanceOfAssertFactories.LONG).isBetween(9_000L, 11_000L);
			});
		});
	}

	/**
	 * Revenue broken down by customer market segment — verifies the JOIN path lineitem → orders → customer reaches
	 * {@code c_mktsegment}.
	 */
	@Test
	public void testRevenue_byMarketSegment() {
		ITabularView result =
				cube().execute(CubeQuery.builder().measure("revenue").groupByAlso("c_mktsegment").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		// TPC-H defines 5 market segments: AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(5)
				.hasEntrySatisfying(Map.of("c_mktsegment", "BUILDING"), v -> {
					Assertions.assertThat((Map) v).hasEntrySatisfying("revenue", revenue -> {
						Assertions.assertThat(revenue)
								.asInstanceOf(InstanceOfAssertFactories.DOUBLE)
								.isGreaterThan(0.0);
					});
				});
	}

	/**
	 * Revenue broken down by customer region — verifies the full snowflake JOIN path lineitem → orders → customer →
	 * cust_nation → cust_region reaches {@code r_name}.
	 */
	@Test
	public void testRevenue_byRegion() {
		ITabularView result = cube().execute(CubeQuery.builder().measure("revenue").groupByAlso("r_name").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		// TPC-H defines 5 regions: AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(5)
				.hasEntrySatisfying(Map.of("r_name", "EUROPE"), v -> {
					Assertions.assertThat((Map) v).hasEntrySatisfying("revenue", revenue -> {
						Assertions.assertThat(revenue)
								.asInstanceOf(InstanceOfAssertFactories.DOUBLE)
								.isGreaterThan(0.0);
					});
				});
	}

	/**
	 * Best-seller parts by revenue — shows the top parts driving the most revenue. Verifies the JOIN path lineitem →
	 * part reaches {@code p_name}.
	 */
	@Test
	public void testRevenue_byPartName_filtered() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure("revenue", "count(*)")
				.groupByAlso("p_brand")
				.andFilter("p_brand", "Brand#13")
				.build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.hasEntrySatisfying(Map.of("p_brand", "Brand#13"), v -> {
					Assertions.assertThat((Map) v).hasEntrySatisfying("revenue", revenue -> {
						Assertions.assertThat(revenue)
								.asInstanceOf(InstanceOfAssertFactories.DOUBLE)
								.isGreaterThan(0.0);
					}).hasEntrySatisfying("count(*)", count -> {
						Assertions.assertThat(count).asInstanceOf(InstanceOfAssertFactories.LONG).isGreaterThan(0L);
					});
				});
	}

	/**
	 * Revenue filtered to SHIP mode — verifies that a lineitem column filter works correctly.
	 */
	@Test
	public void testRevenue_byShipMode() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure("revenue")
				.groupByAlso("l_shipmode")
				.filter(ColumnFilter.matchIn("l_shipmode", "SHIP", "MAIL"))
				.build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		// Only SHIP and MAIL should appear
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsKey(Map.of("l_shipmode", "SHIP"))
				.containsKey(Map.of("l_shipmode", "MAIL"));
	}

	/**
	 * Case 1 — equivalent strategies: when all groupBys share the same aggregator set, {@link TableQueryV4#streamV3()}
	 * collapses them into a single GROUPING SET (identical to {@link TableQueryV4#asCoveringV3()}). Both paths produce
	 * identical results.
	 *
	 * <p>
	 * This demonstrates that for equal aggregator sets, GROUPING SETS and UNION ALL converge to the same SQL and the
	 * same output.
	 */
	// Number of timed repetitions per strategy — increase for more stable measurements
	private static final int NB_RUNS = 3;

	@Test
	public void testGroupingSets_vs_UnionAll_equivalentCase() throws Exception {
		FilteredAggregator revenueAgg = FilteredAggregator.builder()
				.aggregator(Aggregator.builder()
						.name("revenue")
						.aggregationKey(ExpressionAggregation.KEY)
						.columnName("sum(l_extendedprice * (1 - l_discount))")
						.build())
				.build();

		// Two groupBys sharing the SAME aggregator set → streamV3() collapses into ONE V3 (GROUPING SET)
		TableQueryV4 v4 = TableQueryV4.builder()
				.groupByToAggregator(GroupByColumns.named("c_mktsegment"), revenueAgg)
				.groupByToAggregator(GroupByColumns.named("r_name"), revenueAgg)
				.build();

		List<TableQueryV3> v3s = v4.streamV3().toList();
		// Identical aggregator set → collapses to a single V3 (GROUPING SET, not UNION ALL)
		Assertions.assertThat(v3s).hasSize(1);

		QueryPod queryPod = QueryPod.forTable(table());

		// GROUPING SETS path: single SQL query (asCoveringV3 — used internally by JooqTableWrapper)
		long[] timesGroupingSets = new long[NB_RUNS];
		List<Map<String, ?>> groupingSetsRows = List.of();
		for (int i = 0; i < NB_RUNS; i++) {
			long t = System.currentTimeMillis();
			try (ITabularRecordStream stream = table().streamSlices(queryPod, v4)) {
				groupingSetsRows = stream.toList();
			}
			timesGroupingSets[i] = System.currentTimeMillis() - t;
		}

		// UNION ALL path: same single V3 (no actual UNION ALL when there is only one V3)
		long[] timesUnionAll = new long[NB_RUNS];
		List<Map<String, ?>> unionAllRows = List.of();
		for (int i = 0; i < NB_RUNS; i++) {
			long t = System.currentTimeMillis();
			List<Map<String, ?>> rows = new ArrayList<>();
			for (TableQueryV3 v3 : v3s) {
				try (ITabularRecordStream stream = table().streamSlices(queryPod, v3)) {
					rows.addAll(stream.toList());
				}
			}
			timesUnionAll[i] = System.currentTimeMillis() - t;
			unionAllRows = rows;
		}

		log.info("Equivalent case ({} runs) — GROUPING SETS: last={}ms mean={}ms | UNION ALL: last={}ms mean={}ms",
				NB_RUNS,
				timesGroupingSets[NB_RUNS - 1],
				mean(timesGroupingSets),
				timesUnionAll[NB_RUNS - 1],
				mean(timesUnionAll));

		// 5 market segments + 5 regions
		Assertions.assertThat(groupingSetsRows).hasSize(10);
		Assertions.assertThat(unionAllRows).hasSize(10);
		// Both strategies produce identical results when aggregator sets match
		Assertions.assertThat(groupingSetsRows).containsExactlyInAnyOrderElementsOf(unionAllRows);
	}

	/**
	 * Case 2 — wasteful GROUPING SET: when groupBys have distinct aggregator sets, {@link TableQueryV4#asCoveringV3()}
	 * emits ONE SQL computing the cartesian product (extra columns computed unnecessarily for each grouping), while
	 * {@link TableQueryV4#streamV3()} emits two targeted V3s (UNION ALL) computing only what is actually needed.
	 *
	 * <p>
	 * Concretely: {@code count(*) GROUP BY c_mktsegment} and {@code revenue GROUP BY r_name}. The GROUPING SETS path
	 * also computes {@code revenue GROUP BY c_mktsegment} and {@code count(*) GROUP BY r_name}, which are wasted. The
	 * UNION ALL path issues two focused queries and returns rows with only the relevant aggregator per groupBy.
	 */
	@Test
	public void testGroupingSets_vs_UnionAll_wastefulCase() throws Exception {
		FilteredAggregator countAgg = FilteredAggregator.builder().aggregator(Aggregator.countAsterisk()).build();
		FilteredAggregator revenueAgg = FilteredAggregator.builder()
				.aggregator(Aggregator.builder()
						.name("revenue")
						.aggregationKey(ExpressionAggregation.KEY)
						.columnName("sum(l_extendedprice * (1 - l_discount))")
						.build())
				.build();

		// Two groupBys with DIFFERENT aggregator sets → streamV3() emits two separate V3s (UNION ALL)
		TableQueryV4 v4 = TableQueryV4.builder()
				.groupByToAggregator(GroupByColumns.named("c_mktsegment"), countAgg)
				.groupByToAggregator(GroupByColumns.named("r_name"), revenueAgg)
				.build();

		List<TableQueryV3> v3s = v4.streamV3().toList();
		// Different aggregator sets → two independent V3s (UNION ALL semantics)
		Assertions.assertThat(v3s).hasSize(2);

		QueryPod queryPod = QueryPod.forTable(table());

		// GROUPING SETS path: one SQL computing the wasteful cartesian product
		// → returns count(*) AND revenue for BOTH c_mktsegment and r_name groupings
		long[] timesGroupingSets = new long[NB_RUNS];
		List<Map<String, ?>> groupingSetsRows = List.of();
		for (int i = 0; i < NB_RUNS; i++) {
			long t = System.currentTimeMillis();
			try (ITabularRecordStream stream = table().streamSlices(queryPod, v4)) {
				groupingSetsRows = stream.toList();
			}
			timesGroupingSets[i] = System.currentTimeMillis() - t;
		}

		// UNION ALL path: two SQL queries, each computing only the aggregator needed for its groupBy
		long[] timesUnionAll = new long[NB_RUNS];
		List<Map<String, ?>> unionAllRows = List.of();
		for (int i = 0; i < NB_RUNS; i++) {
			long t = System.currentTimeMillis();
			List<Map<String, ?>> rows = new ArrayList<>();
			for (TableQueryV3 v3 : v3s) {
				try (ITabularRecordStream stream = table().streamSlices(queryPod, v3)) {
					rows.addAll(stream.toList());
				}
			}
			timesUnionAll[i] = System.currentTimeMillis() - t;
			unionAllRows = rows;
		}

		log.info("Wasteful case ({} runs) — GROUPING SETS: last={}ms mean={}ms | UNION ALL: last={}ms mean={}ms",
				NB_RUNS,
				timesGroupingSets[NB_RUNS - 1],
				mean(timesGroupingSets),
				timesUnionAll[NB_RUNS - 1],
				mean(timesUnionAll));

		Assertions.assertThat(unionAllRows).hasSize(10);
		// c_mktsegment rows have only count(*) — no wasted revenue column
		unionAllRows.stream()
				.filter(row -> row.containsKey("c_mktsegment"))
				.forEach(row -> Assertions.assertThat(row)
						.as("UNION ALL row for c_mktsegment has only count(*), no wasteful revenue")
						.containsKey("count(*)")
						.doesNotContainKey("revenue"));
		// r_name rows have only revenue — no wasted count(*) column
		unionAllRows.stream()
				.filter(row -> row.containsKey("r_name"))
				.forEach(row -> Assertions.assertThat(row)
						.as("UNION ALL row for r_name has only revenue, no wasteful count(*)")
						.containsKey("revenue")
						.doesNotContainKey("count(*)"));
	}

	private static long mean(long[] values) {
		return LongStream.of(values).sum() / values.length;
	}
}

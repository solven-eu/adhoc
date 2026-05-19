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
package eu.solven.adhoc.engine;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import eu.solven.adhoc.ATestDagInMemory;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.measure.aggregation.comparable.MaxCombination;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.model.measure.Partitionor;
import eu.solven.adhoc.model.query.groupby.GroupByColumns;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.extern.slf4j.Slf4j;

/**
 * Regression guard: a customer deployed adhoc on a single-vCPU host and hit thread-starvation in a previous version
 * where multiple {@link java.util.concurrent.ThreadPoolExecutor}-style pools could deadlock when parent tasks waited on
 * children submitted to the same fixed-size pool.
 *
 * Current architecture: {@link AdhocUnsafe#mixedPool} is a virtual-thread-per-task executor (effectively unbounded) and
 * {@link AdhocUnsafe#cpuPool} is a {@link ForkJoinPool} which avoids the same trap via {@code helpJoin} — but we want a
 * concrete test pinning that property after the VT migration.
 *
 * This test forces both pools to their minimum size (FJP with a single worker, fresh VT-per-task executor) and
 * exercises a wide+deep concurrent DAG plus a partitioned query. Either completes within the {@link Timeout} or the
 * test fails — there is no silent hang.
 */
@Slf4j
public class TestDagCubeQueryEngine_MinimalPools extends ATestDagInMemory implements IAdhocTestConstants {
	int width = 16;

	/**
	 * Shrink global pools to their minimum: a single-worker FJP for CPU-bound work, and a fresh VT-per-task executor
	 * for mixed work. The VT executor is recreated for test isolation; its parallelism is intrinsically dynamic.
	 */
	@BeforeEach
	public void shrinkPools() {
		AdhocUnsafe.setParallelism(1);
	}

	/**
	 * Restore the global pools so this test does not leak shrunken pools to sibling test classes sharing the same
	 * surefire JVM.
	 */
	@AfterEach
	public void restorePools() {
		AdhocUnsafe.resetAll();
	}

	@Override
	@BeforeEach
	public void feedTable() {
		IntStream.range(0, width).forEach(i -> {
			table().add(Map.of("a", "a" + (i % 2), "b", "b" + (i % 3), "k", (long) i, "k_" + i, (long) i));
		});
	}

	/**
	 * Wide+deep DAG mirroring {@link TestDagCubeQueryEngine_Concurrent2#testConcurrentTableQueries}: many sibling
	 * combinators share a base aggregator, two layers deep. With the CPU pool at parallelism 1 and the mixed pool as
	 * VT-per-task, the engine must still complete the query — VTs scale on demand, and FJP work-stealing prevents the
	 * single worker from blocking on its own children.
	 */
	@Test
	@Timeout(value = 60, unit = TimeUnit.SECONDS)
	public void testWideConcurrentDag_minimalPools_doesNotStarve() {
		IntStream.range(0, width).forEach(i -> {
			forest.addMeasure(Aggregator.builder()
					.name("A_" + i)
					.columnName("k_" + i)
					.aggregationKey(SumAggregation.KEY)
					.build());
		});

		List<String> aggregators = IntStream.range(0, width).mapToObj(i -> "A_" + i).toList();
		IntStream.range(0, width).forEach(i -> {
			forest.addMeasure(Combinator.builder()
					.name("C_0_" + i)
					.underlyings(aggregators)
					.combinationKey(SumAggregation.KEY)
					.build());
		});

		List<String> combinators0 = IntStream.range(0, width).mapToObj(i -> "C_0_" + i).toList();
		IntStream.range(0, width).forEach(i -> {
			forest.addMeasure(Combinator.builder()
					.name("C_1_" + i)
					.underlyings(combinators0)
					.combinationKey(SumAggregation.KEY)
					.build());
		});

		List<String> combinators1 = IntStream.range(0, width).mapToObj(i -> "C_1_" + i).toList();
		forest.addMeasure(
				Combinator.builder().name("D").underlyings(combinators1).combinationKey(SumAggregation.KEY).build());

		// The assertion is implicit: completion within the @Timeout proves no starvation.
		ITabularView view =
				cube().execute(CubeQuery.builder().measure("D").option(StandardQueryOptions.CONCURRENT).build());

		Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues()).hasSize(1);
	}

	/**
	 * Partitioned execution path: a {@link Partitionor} combined with {@link StandardQueryOptions#PARTITIONED} routes
	 * per-partition work through {@code adhocCpuPool} (see {@code PartitionedMultitypeMergeableGrid#closeColumn} and
	 * {@code APartitionedColumn}). With that FJP at a single worker, the query must still complete.
	 */
	@Test
	@Timeout(value = 60, unit = TimeUnit.SECONDS)
	public void testPartitionedQuery_minimalPools_doesNotStarve() {
		forest.addMeasure(Partitionor.builder()
				.name("maxK_byB")
				.underlyings(Arrays.asList("k"))
				.groupBy(GroupByColumns.named("b"))
				.combinationKey(MaxCombination.KEY)
				.aggregationKey(SumAggregation.KEY)
				.build());
		forest.addMeasure(Aggregator.builder().name("k").aggregationKey(SumAggregation.KEY).build());

		ITabularView view = cube().execute(CubeQuery.builder()
				.measure("maxK_byB")
				.option(StandardQueryOptions.CONCURRENT)
				.option(StandardQueryOptions.PARTITIONED)
				.build());

		Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues()).hasSize(1);
	}
}

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

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ATestDagInMemory;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.measure.lambda.LambdaCombination;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.util.AdhocBenchmark;

/**
 * Pins the cuboid pruning behaviour of {@link CubeQueryEngine#walkUpDag}: intermediate cuboids are dropped from the
 * in-flight {@code queryStepToValues} map as soon as every downstream consumer is done. A long chain of
 * same-cardinality combinators produces one cuboid per step; without pruning the whole chain stays in memory and the
 * JVM runs out of heap (default {@code -Xmx512M} as set in the root pom). With pruning, peak memory is bounded by the
 * chain's branch width (here 1 — two cuboids alive at any time: the input and the freshly-computed output).
 */
@AdhocBenchmark
public class TestDagCubeQuery_LongChainPruning extends ATestDagInMemory {
	// Cardinality of the groupBy column. Each per-step cuboid carries this many cells.
	private static final int N_ROWS = 10_000;
	// Length of the +1 combinator chain. Holding the full chain at N_ROWS cells per step (no
	// pruning) accumulates CHAIN_LENGTH * N_ROWS = 100M cells in memory simultaneously, easily
	// blowing through the default 512 MiB heap. With pruning, only the two cuboids on either
	// side of the current step survive.
	private static final int CHAIN_LENGTH = 10_000;
	private static final String K = "k";
	private static final String V = "v";

	@Override
	@BeforeEach
	public void feedTable() {
		for (int i = 0; i < N_ROWS; i++) {
			table().add(Map.of(K, i, V, 1));
		}
	}

	@BeforeEach
	public void registerMeasures() {
		forest.addMeasure(Aggregator.builder().name(V).aggregationKey(SumAggregation.KEY).build());
		// Lambda-based combination is ~1000x faster than EvalEx for a trivial `+1`. The chain has
		// CHAIN_LENGTH * N_ROWS cell evaluations; with EvalEx this dominates the test runtime by
		// orders of magnitude over the pruning behaviour we actually want to pin.
		LambdaCombination.ILambdaCombination plus1 = (slice, values) -> {
			Object v = values.get(0);
			if (v == null) {
				return null;
			} else {
				return ((Number) v).longValue() + 1L;
			}
		};
		for (int i = 0; i < CHAIN_LENGTH; i++) {
			String underlying;
			if (i == 0) {
				underlying = V;
			} else {
				underlying = "c" + (i - 1);
			}
			forest.addMeasure(Combinator.builder()
					.name("c" + i)
					.underlying(underlying)
					.combinationKey(LambdaCombination.class.getName())
					.combinationOptions(Map.of(LambdaCombination.K_LAMBDA, plus1))
					.build());
		}
	}

	// Without DAG-iteration pruning of intermediate cuboids, this test runs out of heap before the
	// chain completes (CHAIN_LENGTH cuboids of N_ROWS cells held in memory simultaneously). With
	// pruning, only the two cuboids on either side of the current step survive — peak heap stays
	// comfortably under the default surefire `-Xmx512M`.
	@Test
	public void testLongChain_intermediateCuboidsArePruned() {
		String tail = "c" + (CHAIN_LENGTH - 1);

		CubeQuery query = CubeQuery.builder().measure(tail).groupByAlso(K).build();
		ITabularView view = cube().execute(query);
		MapBasedTabularView mapBased = MapBasedTabularView.load(view);

		// Each row contributes v=1 (single source row per unique k), so SUM(v)=1 at the bottom of
		// the chain; each step adds 1, so the tail measure equals 1 + CHAIN_LENGTH per row.
		long expected = 1L + CHAIN_LENGTH;
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(N_ROWS)
				.containsEntry(Map.of(K, 0L), Map.of(tail, expected))
				.containsEntry(Map.of(K, (long) (N_ROWS - 1)), Map.of(tail, expected));
	}
}

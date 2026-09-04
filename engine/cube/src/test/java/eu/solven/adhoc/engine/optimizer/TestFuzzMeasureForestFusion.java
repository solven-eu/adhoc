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
package eu.solven.adhoc.engine.optimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.eventbus.EventBus;

import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.AdhocTestHelper;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.IQueryStepsDagBuilder;
import eu.solven.adhoc.engine.QueryStepsDagBuilder;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.context.StandardQueryPreparator;
import eu.solven.adhoc.engine.dag.fuser.NoopDagFuser;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.eventbus.AdhocEventBusHelpersUnsafe;
import eu.solven.adhoc.eventbus.IAdhocEventBus;
import eu.solven.adhoc.factories.AdhocFactories;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MaxCombination;
import eu.solven.adhoc.measure.aggregation.comparable.MinAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MinCombination;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.measure.forest.UnsafeMeasureForest;
import eu.solven.adhoc.measure.sum.DivideCombination;
import eu.solven.adhoc.measure.sum.ProductCombination;
import eu.solven.adhoc.measure.sum.SubstractionCombination;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.model.measure.Filtrator;
import eu.solven.adhoc.model.measure.IMeasure;
import eu.solven.adhoc.model.measure.Partitionor;
import eu.solven.adhoc.model.measure.Shiftor;
import eu.solven.adhoc.model.measure.Unfiltrator;
import eu.solven.adhoc.model.query.groupby.GroupByColumns;
import eu.solven.adhoc.table.InMemoryTable;
import lombok.extern.slf4j.Slf4j;

/**
 * Fuzz test: builds a random forest of measures, executes the same query on two cubes — one with {@link NoopDagFuser}
 * and one with the default fuser chain — and asserts the {@link ITabularView}s match.
 *
 * <p>
 * The property being tested: <em>the fuser must not change observable behaviour</em>. Any divergence between the two
 * views is a fuser bug (operand-swap, lost slice, wrong combination, …) — or, less likely, a regression in the raw
 * engine that only the fuser path masked.
 *
 * <p>
 * The generator emits a mix of foldable shapes (Combinator chains and trees), non-foldable carriers (Shiftor — a
 * documented "intentionally not foldable" measure — and Partitionor with non-linear aggregation MAX/MIN), and
 * heterogeneous filter/groupBy (Filtrator with random column filter, Unfiltrator, Partitionor's own groupBy) to
 * exercise the fuser's preflight bail-out path. DAG sharing — a foldable internal referenced by more than one parent —
 * is exercised by reusing existing measure names instead of always recursing into a fresh subtree.
 *
 * <p>
 * Seed control: set {@code -Dfuzz.seed=N} to replay a specific run; default is {@link System#nanoTime()}, printed at
 * INFO before any iteration. Iteration count: {@code -Dfuzz.iterations=N}, default 50 — large enough to surface most
 * pattern combinations in a few seconds, small enough to live in the regular surefire run.
 *
 * <p>
 * Out of scope (future work): composite cubes, larger tables, multi-cube fan-out — those are orthogonal failure modes
 * that deserve their own fuzz harnesses.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class TestFuzzMeasureForestFusion {

	private static final int DEFAULT_ITERATIONS = 50;
	private static final int MAX_DEPTH = 4;

	private static final List<String> CATEGORICAL_COLS = List.of("a", "b");
	private static final List<String> NUMERIC_COLS = List.of("v1", "v2");
	private static final Map<String, List<String>> COLUMN_VALUES =
			Map.of("a", List.of("a1", "a2", "a3"), "b", List.of("b1", "b2"));

	/** Base aggregator names referenced by the generator. */
	private static final List<String> BASE_AGGREGATOR_NAMES =
			List.of("v1_sum", "v1_max", "v1_min", "v2_sum", "v2_max", "v2_min", "count");

	private static final List<Aggregator> BASE_AGGREGATORS =
			List.of(Aggregator.sum("v1").toBuilder().name("v1_sum").build(),
					Aggregator.builder().name("v1_max").columnName("v1").aggregationKey(MaxAggregation.KEY).build(),
					Aggregator.builder().name("v1_min").columnName("v1").aggregationKey(MinAggregation.KEY).build(),
					Aggregator.sum("v2").toBuilder().name("v2_sum").build(),
					Aggregator.builder().name("v2_max").columnName("v2").aggregationKey(MaxAggregation.KEY).build(),
					Aggregator.builder().name("v2_min").columnName("v2").aggregationKey(MinAggregation.KEY).build(),
					Aggregator.countAsterisk().toBuilder().name("count").build());

	private static final List<String> COMBINATION_KEYS = List.of(SumCombination.KEY,
			CoalesceCombination.KEY,
			ProductCombination.KEY,
			DivideCombination.KEY,
			SubstractionCombination.KEY,
			MaxCombination.KEY,
			MinCombination.KEY);

	/** Aggregations the fuzzer may pick for Partitionor. */
	private static final List<String> PARTITIONOR_AGG_KEYS =
			List.of(SumAggregation.KEY, MaxAggregation.KEY, MinAggregation.KEY);

	/**
	 * Relative tolerance when comparing floating-point outputs. The fuser may reorder associative operations (e.g. the
	 * operands of a SUM feeding a DIVIDE), which legitimately perturbs the last ULPs of a double without changing the
	 * semantics.
	 */
	private static final double RELATIVE_TOLERANCE = 1e-12;

	@Test
	public void testFuzz_fuserPreservesSemantics() {
		long seed = Long.getLong("fuzz.seed", System.nanoTime());
		int iterations = Integer.getInteger("fuzz.iterations", DEFAULT_ITERATIONS);
		log.info("Fuzz seed={} iterations={}", seed, iterations);

		InMemoryTable table = seededTable();
		EventBus guava = AdhocTestHelper.eventBus();
		IAdhocEventBus eventBus = AdhocEventBusHelpersUnsafe.safeWrapper(guava::post);
		AdhocFactories factories = AdhocFactories.builder().build();

		CubeQueryEngine noopEngine = new CubeQueryEngine(factories, eventBus, null) {
			@Override
			protected IQueryStepsDagBuilder makeQueryStepsDagsBuilder(QueryPod queryPod) {
				IQueryStepsDagBuilder builder = super.makeQueryStepsDagsBuilder(queryPod);
				((QueryStepsDagBuilder) builder).withOptimizer(new NoopDagFuser());
				return builder;
			}
		};
		CubeQueryEngine defaultEngine = CubeQueryEngine.builder().eventBus(eventBus).factories(factories).build();

		Random rng = new Random(seed);
		for (int i = 0; i < iterations; i++) {
			runOneIteration(rng, seed, i, table, noopEngine, defaultEngine, eventBus);
		}
	}

	/** One generate→execute-twice→compare round. Any mismatch fails with the seed+iteration for replay. */
	private void runOneIteration(Random rng,
			long seed,
			int iteration,
			InMemoryTable table,
			CubeQueryEngine noopEngine,
			CubeQueryEngine defaultEngine,
			IAdhocEventBus eventBus) {
		Generator gen = new Generator(rng);
		String rootName = gen.generate();
		UnsafeMeasureForest forest = UnsafeMeasureForest.builder().name("fuzz_" + iteration).build();
		BASE_AGGREGATORS.forEach(forest::addMeasure);
		gen.getMeasures().forEach(forest::addMeasure);

		CubeWrapper noopCube = CubeWrapper.builder()
				.table(table)
				.engine(noopEngine)
				.forest(forest)
				.eventBus(eventBus)
				.queryPreparator(StandardQueryPreparator.builder().build())
				.build();
		CubeWrapper defaultCube = CubeWrapper.builder()
				.table(table)
				.engine(defaultEngine)
				.forest(forest)
				.eventBus(eventBus)
				.queryPreparator(StandardQueryPreparator.builder().build())
				.build();

		CubeQuery query = CubeQuery.builder().measure(rootName).build();

		ITabularView noopView;
		ITabularView fusedView;
		try {
			noopView = noopCube.execute(query);
			fusedView = defaultCube.execute(query);
		} catch (RuntimeException e) {
			// A generator-produced forest can be semantically invalid (e.g. divide-by-zero, unsupported combination).
			// Both branches throw together → not a fuser bug; skip the iteration. If only one branch throws, that's
			// a divergence and falls into the explicit fail() below via a second execute attempt.
			RuntimeException noopFailure = tryExecute(noopCube, query);
			RuntimeException fusedFailure = tryExecute(defaultCube, query);
			if (noopFailure != null && fusedFailure != null && noopFailure.getClass().equals(fusedFailure.getClass())) {
				return;
			}
			Assertions.fail(String.format("Divergent exception (seed=%d iter=%d root=%s): noop=%s, fused=%s%nforest=%s",
					seed,
					iteration,
					rootName,
					noopFailure,
					fusedFailure,
					describeForest(gen.getMeasures())));
			return;
		}

		Map<Map<String, ?>, Map<String, ?>> noopMap = MapBasedTabularView.load(noopView).getCoordinatesToValues();
		Map<Map<String, ?>, Map<String, ?>> fusedMap = MapBasedTabularView.load(fusedView).getCoordinatesToValues();

		if (!sameWithinTolerance(noopMap, fusedMap)) {
			Assertions.fail(
					String.format("Fuser changed output (seed=%d iter=%d root=%s):%n  noop=%s%n  fused=%s%n  forest=%s",
							seed,
							iteration,
							rootName,
							noopMap,
							fusedMap,
							describeForest(gen.getMeasures())));
		}
	}

	private RuntimeException tryExecute(CubeWrapper cube, CubeQuery query) {
		try {
			cube.execute(query);
			return null;
		} catch (RuntimeException e) {
			return e;
		}
	}

	/**
	 * Compares two views coordinate by coordinate.
	 *
	 * @return true if both views hold the same coordinates and, per coordinate, the same measure values, where
	 *         floating-point numbers are compared within {@link #RELATIVE_TOLERANCE} and every other value with
	 *         {@link Objects#equals(Object, Object)}.
	 */
	private static boolean sameWithinTolerance(Map<Map<String, ?>, Map<String, ?>> left,
			Map<Map<String, ?>, Map<String, ?>> right) {
		if (!left.keySet().equals(right.keySet())) {
			return false;
		}
		for (Map.Entry<Map<String, ?>, Map<String, ?>> coordinate : left.entrySet()) {
			Map<String, ?> leftValues = coordinate.getValue();
			Map<String, ?> rightValues = right.get(coordinate.getKey());
			if (!leftValues.keySet().equals(rightValues.keySet())) {
				return false;
			}
			for (Map.Entry<String, ?> value : leftValues.entrySet()) {
				if (!sameWithinTolerance(value.getValue(), rightValues.get(value.getKey()))) {
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * @return true if both values are equal, allowing a relative difference of {@link #RELATIVE_TOLERANCE} when either
	 *         side is a floating-point number.
	 */
	private static boolean sameWithinTolerance(Object left, Object right) {
		boolean anyFloating =
				left instanceof Double || left instanceof Float || right instanceof Double || right instanceof Float;
		if (!anyFloating) {
			return Objects.equals(left, right);
		}
		// Guard clause rather than `else if`: the pattern variables must stay in scope for the arithmetic below
		if (!(left instanceof Number leftNumber) || !(right instanceof Number rightNumber)) {
			return false;
		}
		double leftDouble = leftNumber.doubleValue();
		double rightDouble = rightNumber.doubleValue();
		// `Double.compare` handles NaN==NaN and identical infinities, which the arithmetic below would reject
		if (Double.compare(leftDouble, rightDouble) == 0) {
			return true;
		}
		double magnitude = Math.max(Math.abs(leftDouble), Math.abs(rightDouble));
		return Math.abs(leftDouble - rightDouble) <= RELATIVE_TOLERANCE * magnitude;
	}

	private String describeForest(List<IMeasure> measures) {
		StringBuilder sb = new StringBuilder().append('[');
		for (IMeasure m : measures) {
			sb.append("\n  ").append(m);
		}
		return sb.append("\n]").toString();
	}

	/** Deterministic 12-row table — same instance for every iteration. */
	private InMemoryTable seededTable() {
		InMemoryTable t = InMemoryTable.builder().name("fuzz_table").build();
		t.add(Map.of("a", "a1", "b", "b1", "v1", 10L, "v2", 5L));
		t.add(Map.of("a", "a1", "b", "b1", "v1", 20L, "v2", 3L));
		t.add(Map.of("a", "a1", "b", "b2", "v1", 15L, "v2", 8L));
		t.add(Map.of("a", "a2", "b", "b1", "v1", 7L, "v2", 12L));
		t.add(Map.of("a", "a2", "b", "b1", "v1", 30L, "v2", 4L));
		t.add(Map.of("a", "a2", "b", "b2", "v1", 25L, "v2", 9L));
		t.add(Map.of("a", "a3", "b", "b1", "v1", 5L, "v2", 6L));
		t.add(Map.of("a", "a3", "b", "b2", "v1", 12L, "v2", 11L));
		t.add(Map.of("a", "a3", "b", "b2", "v1", 18L, "v2", 2L));
		t.add(Map.of("a", "a1", "b", "b2", "v1", 22L, "v2", 7L));
		t.add(Map.of("a", "a2", "b", "b2", "v1", 8L, "v2", 13L));
		t.add(Map.of("a", "a3", "b", "b1", "v1", 14L, "v2", 1L));
		return t;
	}

	/** Recursive measure-tree generator. Captures the list of created measures so the test can re-print on failure. */
	@lombok.RequiredArgsConstructor
	@lombok.Getter
	private static final class Generator {
		private final Random rng;
		private final List<IMeasure> measures = new ArrayList<>();
		private int nextId;

		String generate() {
			return makeMeasure(0);
		}

		private String makeMeasure(int depth) {
			// Leaf probability grows with depth so the generator terminates.
			double leafProb = 0.15 + 0.25 * depth;
			if (depth >= MAX_DEPTH || rng.nextDouble() < leafProb) {
				return pick(rng, BASE_AGGREGATOR_NAMES);
			}

			// Pick measure type. Combinator and Filtrator are the heaviest hitters (most likely to fold or to bail
			// out the fuser's heterogeneous-filter preflight). Shiftor exercises the non-Combinator carrier path.
			double roll = rng.nextDouble();
			String name = "m" + nextId++;
			if (roll < 0.45) {
				// Combinator with 1..3 underlyings.
				int numChildren = 1 + rng.nextInt(3);
				List<String> children = new ArrayList<>();
				for (int j = 0; j < numChildren; j++) {
					children.add(childMeasure(depth + 1));
				}
				IMeasure m = Combinator.builder()
						.name(name)
						.underlyings(children)
						.combinationKey(pick(rng, COMBINATION_KEYS))
						.build();
				measures.add(m);
			} else if (roll < 0.6) {
				// Filtrator on a single underlying with a random column filter — exercises heterogeneous-filter path.
				String column = pick(rng, CATEGORICAL_COLS);
				String value = pick(rng, COLUMN_VALUES.get(column));
				IMeasure m = Filtrator.builder()
						.name(name)
						.underlying(childMeasure(depth + 1))
						.filter(ColumnFilter.matchEq(column, value))
						.build();
				measures.add(m);
			} else if (roll < 0.75) {
				// Unfiltrator on a single underlying — Suppress on a categorical column.
				IMeasure m = Unfiltrator.builder()
						.name(name)
						.underlying(childMeasure(depth + 1))
						.column(pick(rng, CATEGORICAL_COLS))
						.build();
				measures.add(m);
			} else if (roll < 0.9) {
				// Partitionor with non-linear aggregation (MAX/MIN/SUM) on a random groupBy.
				int numUnder = 1 + rng.nextInt(2);
				List<String> children = new ArrayList<>();
				for (int j = 0; j < numUnder; j++) {
					children.add(childMeasure(depth + 1));
				}
				IMeasure m = Partitionor.builder()
						.name(name)
						.underlyings(children)
						.groupBy(GroupByColumns.named(pick(rng, CATEGORICAL_COLS)))
						.combinationKey(pick(rng, COMBINATION_KEYS))
						.aggregationKey(pick(rng, PARTITIONOR_AGG_KEYS))
						.build();
				measures.add(m);
			} else {
				// Shiftor — single underlying, shifts a categorical column to a fixed value. Intentionally
				// non-foldable per `docs/optimization.md`.
				String column = pick(rng, CATEGORICAL_COLS);
				String value = pick(rng, COLUMN_VALUES.get(column));
				IMeasure m = Shiftor.builder()
						.name(name)
						.underlying(childMeasure(depth + 1))
						.editorKey(SimpleFilterEditor.KEY)
						.editorOptions(Map.of(SimpleFilterEditor.P_SHIFTED, Map.of(column, value)))
						.build();
				measures.add(m);
			}
			return name;
		}

		/**
		 * Either recurse into a fresh subtree, or reuse an existing measure (creating a multi-consumer DAG).
		 * Existing-reuse is gated on {@code p=0.2} to keep the DAG mostly tree-shaped but with occasional sharing — the
		 * exact case the multi-consumer-top relaxation now handles.
		 */
		private String childMeasure(int depth) {
			if (!measures.isEmpty() && rng.nextDouble() < 0.2) {
				return measures.get(rng.nextInt(measures.size())).getName();
			}
			return makeMeasure(depth);
		}
	}

	private static <T> T pick(Random rng, List<T> options) {
		return options.get(rng.nextInt(options.size()));
	}
}

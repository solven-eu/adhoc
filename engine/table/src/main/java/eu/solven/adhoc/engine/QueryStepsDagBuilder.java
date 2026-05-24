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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.NonNull;

import eu.solven.adhoc.engine.cache.IQueryStepCache;
import eu.solven.adhoc.engine.dag.fuser.CombinatorSubgraphsFuser;
import eu.solven.adhoc.engine.dag.fuser.CompositeDagFuser;
import eu.solven.adhoc.engine.dag.fuser.FiltratorToCombinatorFuser;
import eu.solven.adhoc.engine.dag.fuser.IQueryStepsDagFuser;
import eu.solven.adhoc.engine.dag.fuser.PartitionorToCombinatorFuser;
import eu.solven.adhoc.engine.dag.fuser.UnfiltratorToCombinatorFuser;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.IHasTransverseCache;
import eu.solven.adhoc.engine.step.IWhereGroupByQuery;
import eu.solven.adhoc.factories.IAdhocFactories;
import eu.solven.adhoc.measure.forest.IMeasureResolver;
import eu.solven.adhoc.model.measure.IMeasure;
import eu.solven.adhoc.table.IQueryPod;
import lombok.extern.slf4j.Slf4j;

/**
 * Two-phase {@link QueryStepsDag} producer.
 *
 * <p>
 * Phase 1 (delegated to {@link InitialQueryStepsDagBuilder}): accumulate per-measure steps into the in-flight
 * multigraph + DAG. Phase 2 (this class): apply the configured {@link IQueryStepsDagFuser} to the un-fused DAG and
 * publish the fused result.
 *
 * <p>
 * Splitting the two phases keeps the accumulation state fully {@code final} and confines the mutable references
 * required by fusion to this wrapper.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class QueryStepsDagBuilder implements IQueryStepsDagBuilder, IHasTransverseCache {

	protected final InitialQueryStepsDagBuilder initial;

	// Pluggable DAG-level fuser run after the DAG is fully accumulated and before getQueryDag() returns. Default
	// chains the four built-in rewrites (see CompositeDagFuser). Override via withOptimizer(...) for tests or for
	// projects with their own rules; pass NoopDagFuser to disable.
	//
	// BEWARE — Shiftor is intentionally absent from this chain even when its IFilterEditor would leave the step filter
	// unchanged. ShiftorQueryStep.processSlicesMaterializedByFilters synthesizes slices for coordinates explicitly
	// pinned in the user filter (Equals/In) but missing from the table, emitting a `null` value for them. A passthrough
	// Combinator(COALESCE) would not — those slices would simply be absent from the output. That divergence is real
	// when the table has gaps on user-pinned coordinates, so a Shiftor → Combinator rewrite is unsafe in the general
	// case. TODO: lift the user-filter-slice synthesis to a single engine-level post-processing pass shared across
	// measure types; once Shiftor no longer owns this concern, the rewrite becomes safe and can be added here.
	@NonNull
	protected IQueryStepsDagFuser optimizer = new CompositeDagFuser(new PartitionorToCombinatorFuser(),
			new FiltratorToCombinatorFuser(),
			new UnfiltratorToCombinatorFuser(),
			new CombinatorSubgraphsFuser());

	public QueryStepsDagBuilder(InitialQueryStepsDagBuilder initial) {
		this.initial = initial;
	}

	/** Convenience constructor: wraps a freshly-built {@link InitialQueryStepsDagBuilder}. */
	public QueryStepsDagBuilder(IAdhocFactories factories,
			IMeasureResolver canResolveMeasures,
			IWhereGroupByQuery query,
			Set<IMeasure> queriedMeasures,
			IQueryStepCache queryStepCache) {
		this(new InitialQueryStepsDagBuilder(factories, canResolveMeasures, query, queriedMeasures, queryStepCache));
	}

	@Override
	public Map<Object, Object> getTransverseCache() {
		return initial.getTransverseCache();
	}

	/**
	 * Replace the default {@link IQueryStepsDagFuser}. Useful for tests that want to inspect the un-fused DAG (pass
	 * {@code NoopDagFuser}) and for projects providing custom rewrite rules.
	 */
	public QueryStepsDagBuilder withOptimizer(IQueryStepsDagFuser optimizer) {
		this.optimizer = optimizer;
		return this;
	}

	/**
	 * Verifies invariants that every {@link IQueryStepsDagFuser} must preserve:
	 * <ul>
	 * <li>every user-requested step is still in the graph with the same identity;</li>
	 * <li>every leaf captured before fusing is still present — a leaf step represents the data the table layer will
	 * fetch (it becomes a {@code TableQueryStep} on the table side) and the cube-level fuser must leave it
	 * untouched;</li>
	 * <li>every pre-cached step is still in the graph — the engine relies on its cuboid being present.</li>
	 * </ul>
	 */
	protected void sanityCheckAfterFusion(QueryStepsDag fused, Set<CubeQueryStep> leavesBeforeFuse) {
		for (CubeQueryStep root : fused.getExplicits()) {
			if (!fused.getMultigraph().containsVertex(root)) {
				throw new IllegalStateException("Fuser removed an explicit (user-requested) step: " + root
						+ ". Fusers must leave roots untouched.");
			}
		}
		for (CubeQueryStep cached : fused.getStepToValues().keySet()) {
			if (!fused.getMultigraph().containsVertex(cached)) {
				throw new IllegalStateException("Fuser removed a pre-cached step: " + cached
						+ ". Fusers must leave cache-loaded steps untouched.");
			}
		}
		for (CubeQueryStep leaf : leavesBeforeFuse) {
			if (!fused.getMultigraph().containsVertex(leaf)) {
				throw new IllegalStateException("Fuser removed a leaf step: " + leaf
						+ ". Fusers must leave table-layer leaves (out-degree 0) untouched.");
			}
		}
	}

	@Override
	public QueryStepsDag makeQueryDag() {
		QueryStepsDag unfused = initial.makeQueryDag();

		// Snapshot the leaves (out-degree 0 vertices) before the fuser runs. A leaf cube-step is the one that will be
		// routed to the table layer as a TableQueryStep; the cube-level fuser must leave them untouched. We capture
		// by identity so the post-fuse sanity check can verify no leaf was dropped or rewired-into-a-non-leaf.
		Set<CubeQueryStep> leavesBeforeFuse = new LinkedHashSet<>();
		for (CubeQueryStep step : unfused.getMultigraph().vertexSet()) {
			if (unfused.getMultigraph().outgoingEdgesOf(step).isEmpty()) {
				leavesBeforeFuse.add(step);
			}
		}

		int sizeBeforeFusion = unfused.getInducedToInducer().vertexSet().size();

		QueryStepsDag fused = optimizer.fuse(unfused);

		int sizeAfterFusion = fused.getInducedToInducer().vertexSet().size();
		log.debug("Fused from {} to {} steps", sizeBeforeFusion, sizeAfterFusion);

		// Post-fuse invariants: a fuser is free to add / remove / rewire intermediate nodes, but it must not touch the
		// structural anchors of the DAG. Re-check them defensively so a buggy custom fuser surfaces with a clear error
		// here, not as a "missing cuboid" deep in the engine.
		sanityCheckAfterFusion(fused, leavesBeforeFuse);

		return fused;
	}

	public static QueryStepsDagBuilder make(IAdhocFactories factories,
			IQueryPod queryPod,
			Set<IMeasure> queriedMeasures) {
		InitialQueryStepsDagBuilder initial = new InitialQueryStepsDagBuilder(factories,
				queryPod::resolveIfRef,
				queryPod.getQuery(),
				queriedMeasures,
				queryPod.getQueryStepCache());
		return new QueryStepsDagBuilder(initial);
	}
}

/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jgrapht.GraphPath;
import org.jgrapht.alg.shortestpath.DijkstraShortestPath;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import eu.solven.adhoc.column.coordinate.ICalculatedCoordinate;
import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.engine.cache.IQueryStepCache;
import eu.solven.adhoc.engine.cache.TransverseCacheHelper;
import eu.solven.adhoc.engine.dag.AdhocDag;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.optimizer.CompositeQueryStepsDagOptimizer;
import eu.solven.adhoc.engine.optimizer.FoldCombinatorSubgraphsOptimizer;
import eu.solven.adhoc.engine.optimizer.IQueryStepsDagOptimizer;
import eu.solven.adhoc.engine.optimizer.PartitionorToCombinatorOptimizer;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.IHasTransverseCache;
import eu.solven.adhoc.engine.step.IWhereGroupByQuery;
import eu.solven.adhoc.exception.AdhocExceptionHelpers;
import eu.solven.adhoc.factories.IAdhocFactories;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.measure.forest.IMeasureResolver;
import eu.solven.adhoc.measure.model.ITableMeasure;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.step.IMeasureQueryStep;
import eu.solven.adhoc.model.column.ColumnWithCalculatedCoordinates;
import eu.solven.adhoc.model.column.FunctionCalculatedColumn;
import eu.solven.adhoc.model.column.IAdhocColumn;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.IMeasure;
import eu.solven.adhoc.model.measure.ReferencedMeasure;
import eu.solven.adhoc.model.query.IGroupBy;
import eu.solven.adhoc.model.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.table.IQueryPod;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps building a {@link QueryStepsDag}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class QueryStepsDagBuilder implements IQueryStepsDagBuilder, IHasTransverseCache {
	final IAdhocFactories factories;
	// final String table;
	final IWhereGroupByQuery query;
	final IQueryStepCache queryStepCache;

	// Linked as this will be used for iterating the output result
	final Set<CubeQueryStep> roots = new LinkedHashSet<>();

	// The DAG maintain the actual query nodes, as it enable topological ordering
	final IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
	// The multigraph enables a queryStep to refer multiple times to the same underlying queryStep
	final DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph = new DirectedMultigraph<>(DefaultEdge.class);

	// Holds the querySteps which underlying steps are pending for processing
	// Not a HashSet as we want FIFO behavior, for reproducibility
	// Not a LinkedList as we'll do many `.contains`
	final Collection<CubeQueryStep> pending = new LinkedHashSet<>();

	// Holds the querySteps which underlying steps are processed
	final Set<CubeQueryStep> processed = new LinkedHashSet<>();

	// From cache
	final Map<CubeQueryStep, ICuboid> stepToValue = new LinkedHashMap<>();

	// Pluggable DAG-level optimizer run after the DAG is fully populated and before getQueryDag() returns. Default
	// is the linear-chain folder (see FoldCombinatorSubgraphsOptimizer). Override via withOptimizer(...) for tests or
	// for
	// projects with their own rules; pass NoopQueryStepsDagOptimizer to disable.
	@NonNull
	IQueryStepsDagOptimizer optimizer = new CompositeQueryStepsDagOptimizer(new PartitionorToCombinatorOptimizer(),
			new FoldCombinatorSubgraphsOptimizer());

	// Used to store transient information, like slow-to-evaluate information
	// Should be a threadSafe implementation
	// It is a unique instance, available to all CubeQuerySteps
	@NonNull
	ConcurrentMap<Object, Object> crossStepsCache = new ConcurrentHashMap<>();

	final IMeasureResolver measureResolver;

	public QueryStepsDagBuilder(IAdhocFactories factories,
			// String cube,
			IMeasureResolver canResolveMeasures,
			IWhereGroupByQuery query,
			IQueryStepCache queryStepCache) {
		this.factories = factories;
		// this.table = cube;
		this.measureResolver = canResolveMeasures;
		this.query = query;
		this.queryStepCache = queryStepCache;

		// Rely on cache as this will be used only through a single query
		IFilterOptimizer filterOptimizer = factories.getFilterOptimizerFactory().makeOptimizerWithCache();
		crossStepsCache.put(CubeQueryStep.KEY_FILTER_OPTIMIZER, filterOptimizer);
	}

	@Override
	public Map<Object, Object> getTransverseCache() {
		return crossStepsCache;
	}

	protected void addRoot(IMeasure queriedMeasure) {
		// TODO rootMeasureless(query) should be computed only once for all root measures
		for (MeasurelessQuery rootMeasureless : rootMeasureless()) {
			CubeQueryStep rootStep = CubeQueryStep.edit(rootMeasureless).measure(queriedMeasure).build();

			roots.add(rootStep);
			if (addVertex(rootStep)) {
				pending.add(rootStep);
			}
		}
	}

	/**
	 * If the query is simple, this holds a single MeasurelessQuery. However, some groupedBy columns would imply
	 * additional {@link MeasurelessQuery} (e.g. some column, if groupedBy, adds calculated coordinates which are
	 * additional filters.)
	 * 
	 * @return a {@link Set} of {@link MeasurelessQuery}.
	 */
	protected Set<MeasurelessQuery> rootMeasureless() {
		// May refer some calculatedCoordinates as groupBy
		NavigableMap<String, IAdhocColumn> nameToColumn = query.getGroupBy().getSortedNameToColumn();

		// Each index is associated to a groupedBy column
		// Each groupedBy column is associated to the list of column definitions
		// Default case is to have a simple `groupBy`. We have an additional groupBy definition per calculated
		// coordinate
		// This is later used to do a cartesian product, between all columns, each column being associated to its
		// calculated coordinates
		List<List<Map.Entry<IAdhocColumn, ISliceFilter>>> indexToGroupBys = new ArrayList<>();

		nameToColumn.values().forEach(column -> {
			if (column instanceof ColumnWithCalculatedCoordinates hasCalculated) {
				List<Map.Entry<IAdhocColumn, ISliceFilter>> subColumns = new ArrayList<>();

				// Suppress any natural row whose coordinate value collides with a declared calculated coordinate.
				// A calculated coordinate carries its own filter (it is a functional rewrite of that slice), so a
				// row produced by the natural query for the same key would clash on merge — same slice key, same
				// aggregator name — and `MapBasedTabularView.appendSlice` would crash on the duplicate. Excluding
				// the colliding values from the natural query makes the calculated coordinate authoritative for
				// its slice.
				//
				// The conventional grand-total marker `*` is filtered out of the suppression set: it is never a
				// value any real row carries (the natural rows are LocalDate, Integer, etc., not the literal
				// String "*"), so the filter would always be a no-op semantically — but emitting it as SQL
				// (`d != cast(? as varchar)` with ? = "*") triggers a type-coercion error on typed columns
				// (DuckDB: "invalid date field format"). Skipping `*` preserves the legacy grand-total pattern.
				ImmutableSet<Object> suppressedValues = hasCalculated.getCalculatedCoordinates()
						.stream()
						.map(ICalculatedCoordinate::getCoordinate)
						.filter(coordinate -> !"*".equals(coordinate))
						.collect(ImmutableSet.toImmutableSet());
				ISliceFilter naturalFilter;
				if (suppressedValues.isEmpty()) {
					naturalFilter = ISliceFilter.MATCH_ALL;
				} else {
					naturalFilter = ColumnFilter.notIn(column.getName(), suppressedValues);
				}
				subColumns.add(Map.entry(hasCalculated.getColumn(), naturalFilter));

				// Add each additional coordinate
				List<Map.Entry<IAdhocColumn, ISliceFilter>> list =
						hasCalculated.getCalculatedCoordinates().stream().map(calculatedCoordinate -> {
							IAdhocColumn staticValueColumn = FunctionCalculatedColumn.builder()
									.name(column.getName())
									.recordToCoordinate(
											FunctionCalculatedColumn.constant(calculatedCoordinate.getCoordinate()))
									// `skipFiltering` feels like bad-design. It is used to prevent
									// `ColumnsManager.openTableStream`
									// rejecting a calculatedColumn being filtered, as these calculatedCoordinates
									// should always be included. We may argue these calculatedColumns should not even
									// be visible by `ColumnsManager.openTableStream`.
									.skipFiltering(true)
									.build();
							return Map.entry(staticValueColumn, calculatedCoordinate.getFilter());
						}).toList();
				subColumns.addAll(list);

				indexToGroupBys.add(subColumns);
			} else {
				indexToGroupBys.add(ImmutableList.of(Map.entry(column, ISliceFilter.MATCH_ALL)));
			}
		});

		// Use the global default if no transverse-cache optimizer was registered for this DAG.
		IFilterOptimizer filterOptimizer = Objects.requireNonNullElse(TransverseCacheHelper.getFilterOptimizer(this),
				eu.solven.adhoc.filter.AdhocFilterUnsafe.filterOptimizer);

		return Lists.cartesianProduct(indexToGroupBys).stream().map(columns -> {
			IGroupBy groupBy = GroupByColumns.of(columns.stream().map(Map.Entry::getKey).toList());
			ISliceFilter andFilter =
					FilterBuilder.and(columns.stream().map(Map.Entry::getValue).toList()).optimize(filterOptimizer);
			return MeasurelessQuery.edit(query)
					.groupBy(groupBy)
					.filter(FilterBuilder.and(query.getFilter(), andFilter).optimize(filterOptimizer))
					.build();
		}).collect(ImmutableSet.toImmutableSet());
	}

	/**
	 * 
	 * @param step
	 * @return `true` if the vertex underlyings step should be added. `false` if the vertex has already been
	 *         encountered, or if the cache has hit.
	 */
	protected boolean addVertex(CubeQueryStep step) {
		boolean hasCache;

		if (stepToValue.containsKey(step)) {
			hasCache = true;
		} else {
			Optional<ICuboid> optCuboid = queryStepCache.getValue(step);
			if (optCuboid.isPresent()) {
				stepToValue.put(step, optCuboid.get());

				if (step.isDebugOrExplain()) {
					log.info("[EXPLAIN] step from cache: {}", step);
				}

				// The vertex must be added as even if we have a cacheHit, the DAG may need to refer to it for other
				// measures.
				hasCache = true;
			} else {
				hasCache = false;
			}
		}

		boolean addedDag = dag.addVertex(step);
		boolean addedMultigraph = multigraph.addVertex(step);

		if (addedDag != addedMultigraph) {
			throw new IllegalStateException("Inconsistent vertices around step=%s".formatted(step));
		}

		// BEWARE Is this bad-design? Should the transverseCache be in its own field?
		step.setCrossStepsCache(crossStepsCache);

		if (hasCache) {
			// result from cache : no need to request for underlyings
			return false;
		}

		return addedDag;
	}

	protected boolean hasLeftovers() {
		return !pending.isEmpty();
	}

	protected @Nullable CubeQueryStep pollLeftover() {
		// Equivalent with `Deque.poll()`
		if (pending.isEmpty()) {
			return null;
		} else {
			Iterator<CubeQueryStep> iterator = pending.iterator();
			CubeQueryStep polled = iterator.next();
			iterator.remove();
			return polled;
		}
	}

	/**
	 * 
	 * @param queriedStep
	 *            the queried/parent step
	 * @param underlyingStep
	 *            an underlying step for the queried/parent step
	 */
	protected void registerUnderlying(CubeQueryStep queriedStep, CubeQueryStep underlyingStep) {
		boolean added = addVertex(underlyingStep);
		if (!added) {
			log.debug("underlyingStep already registered step={}", underlyingStep);
		}

		DefaultEdge dagEdge;

		try {
			dagEdge = dag.addEdge(queriedStep, underlyingStep);
		} catch (IllegalArgumentException e) {
			throw buildAddEdgeException(queriedStep, underlyingStep, e);
		}
		if (dagEdge == null) {
			log.debug("One step refers multiple times to same underlying (queried={} underlying={})",
					queriedStep,
					underlyingStep);

		}

		DefaultEdge multigraphEdge = multigraph.addEdge(queriedStep, underlyingStep);
		if (multigraphEdge == null) {
			throw new IllegalStateException(
					"The multigraph implementation should not reject edge-multiplicity>1. queriedStep=%s underlyingStep=%s"
							.formatted(queriedStep, underlyingStep));
		}

	}

	/**
	 * Translates a raw {@link IllegalArgumentException} from {@code dag.addEdge} into a targeted
	 * {@link IllegalStateException} whose message identifies the actual problem: a self-loop, a cycle (with the
	 * offending path), or an unrecognised graph constraint violation.
	 *
	 * @param queriedStep
	 *            the step that was being registered as the parent
	 * @param underlyingStep
	 *            the step that was being registered as the child / dependency
	 * @param cause
	 *            the raw exception thrown by {@code dag.addEdge}
	 * @return an {@link IllegalStateException} ready to be thrown by the caller
	 */
	protected IllegalStateException buildAddEdgeException(CubeQueryStep queriedStep,
			CubeQueryStep underlyingStep,
			IllegalArgumentException cause) {
		// GraphCycleProhibitedException is a subClass of IllegalArgumentException
		// But we may receive IllegalArgumentException
		if (underlyingStep.equals(queriedStep)) {
			return new IllegalStateException("A queryStep can not be its own underlying: `%s`".formatted(queriedStep),
					cause);
		}
		// If there is already a path from underlyingStep to queriedStep, adding queriedStep→underlyingStep
		// closes a cycle. Report that faulty path explicitly instead of dumping the full DAG.
		// Guard containsVertex: if queriedStep was never registered the exception has a different root cause.
		if (dag.containsVertex(queriedStep)) {
			GraphPath<CubeQueryStep, DefaultEdge> existingPath =
					DijkstraShortestPath.findPathBetween(dag, underlyingStep, queriedStep);
			if (existingPath != null) {
				List<CubeQueryStep> cycle = new ArrayList<>();
				cycle.add(queriedStep);
				cycle.addAll(existingPath.getVertexList());
				return new IllegalStateException(
						"Adding edge `%s`->`%s` would create a cycle: %s".formatted(queriedStep, underlyingStep, cycle),
						cause);
			}
		}
		return new IllegalStateException(
				"Issue adding `%s`->`%s` in dag=`%s`".formatted(queriedStep, underlyingStep, dag),
				cause);
	}

	protected void registerUnderlyings(CubeQueryStep parentStep, List<CubeQueryStep> underlyingSteps) {
		underlyingSteps.forEach(underlyingStep -> registerUnderlying(parentStep, underlyingStep));

		// Register the parent as processed
		processed.add(parentStep);
		log.debug("processed: {}", parentStep);

		// Register its underlyings as leftovers, if not already processed
		underlyingSteps.stream().filter(underlyingStep ->
		// If the underlying is already processed: skip it
		!processed.contains(underlyingStep)
				// If the underlying is already pending for processing: skip it
				&& !pending.contains(underlyingStep)).forEach(underlyingStep -> {
					pending.add(underlyingStep);
					log.debug("pending: {}", underlyingStep);
				});
	}

	public void sanityChecks() {
		// sanity check
		dag.vertexSet().forEach(step -> {
			if (step.getMeasure() instanceof ReferencedMeasure ref) {
				throw new IllegalStateException("The DAG must not rely on ReferencedMeasure=%s".formatted(ref));
			}
		});
	}

	@Override
	public QueryStepsDag getQueryDag() {
		return QueryStepsDag.builder()
				.inducedToInducer(dag)
				.multigraph(multigraph)
				.explicits(roots)
				.stepToValues(stepToValue)
				.build();
	}

	@Override
	public void registerRootWithDescendants(Set<IMeasure> queriedMeasures) {
		queriedMeasures.forEach(queriedMeasure -> {
			queriedMeasure = resolveMeasure(queriedMeasure);

			addRoot(queriedMeasure);
		});

		registerDescendants();

		sanityChecks();

		// Snapshot the leaves (out-degree 0 vertices) before the optimizer runs. A leaf cube-step is the one that
		// will be routed to the table layer as a TableQueryStep; the cube-level optimizer must leave them untouched.
		// We capture by identity here so the post-optimization sanity check can verify no leaf was dropped or
		// rewired-into-a-non-leaf.
		Set<CubeQueryStep> leavesBeforeOptimization = new LinkedHashSet<>();
		for (CubeQueryStep step : multigraph.vertexSet()) {
			if (multigraph.outgoingEdgesOf(step).isEmpty()) {
				leavesBeforeOptimization.add(step);
			}
		}

		// Apply pluggable DAG-level optimizations (e.g. folding linear combinator chains). The optimizer mutates
		// `dag` and `multigraph` in place; `roots` and `stepToValue` are read-only inputs that constrain what may
		// be removed. Tests / projects that need the un-optimised shape inject a NoopQueryStepsDagOptimizer via
		// `withOptimizer(...)`.
		optimizer.optimize(multigraph, dag, roots, stepToValue);

		// Post-optimization invariants: an optimizer is free to add / remove / rewire intermediate nodes, but it
		// must not touch the structural anchors of the DAG. Re-check them defensively so a buggy custom optimizer
		// surfaces with a clear error here, not as a "missing cuboid" deep in the engine.
		sanityCheckAfterOptimization(leavesBeforeOptimization);
	}

	/**
	 * Verifies invariants that every {@link IQueryStepsDagOptimizer} must preserve:
	 * <ul>
	 * <li>every step in {@link #roots} (user-requested) is still in the graph with the same identity;</li>
	 * <li>every leaf captured before the optimizer ran is still present — a leaf step represents the data the table
	 * layer will fetch (it becomes a {@code TableQueryStep} on the table side) and the cube-level optimizer must leave
	 * it untouched;</li>
	 * <li>every pre-cached step is still in the graph — the engine relies on its cuboid being present.</li>
	 * </ul>
	 */
	protected void sanityCheckAfterOptimization(Set<CubeQueryStep> leavesBeforeOptimization) {
		for (CubeQueryStep root : roots) {
			if (!multigraph.containsVertex(root)) {
				throw new IllegalStateException("Optimizer removed an explicit (user-requested) step: " + root
						+ ". Optimizers must leave roots untouched.");
			}
		}
		for (CubeQueryStep cached : stepToValue.keySet()) {
			if (!multigraph.containsVertex(cached)) {
				throw new IllegalStateException("Optimizer removed a pre-cached step: " + cached
						+ ". Optimizers must leave cache-loaded steps untouched.");
			}
		}
		for (CubeQueryStep leaf : leavesBeforeOptimization) {
			if (!multigraph.containsVertex(leaf)) {
				throw new IllegalStateException("Optimizer removed a leaf step: " + leaf
						+ ". Optimizers must leave table-layer leaves (out-degree 0) untouched.");
			}
		}
	}

	/**
	 * Replace the default {@link IQueryStepsDagOptimizer} (a {@link FoldCombinatorSubgraphsOptimizer}). Useful for
	 * tests that want to inspect the un-optimised DAG, and for projects providing custom rewrite rules.
	 */
	public QueryStepsDagBuilder withOptimizer(IQueryStepsDagOptimizer optimizer) {
		this.optimizer = optimizer;
		return this;
	}

	protected void registerDescendants() {
		// Add implicitly requested steps
		while (hasLeftovers()) {
			CubeQueryStep queryStep =
					Objects.requireNonNull(pollLeftover(), "hasLeftovers() guarantees a non-null queryStep");

			IMeasure measure = measureResolver.resolveIfRef(queryStep.getMeasure());

			if (measure instanceof Aggregator aggregator) {
				log.debug("Aggregators (here {}) do not have any underlying measure", aggregator);
			} else if (measure instanceof IHasUnderlyingMeasures measureWithUnderlyings) {
				IMeasureQueryStep wrappedQueryStep =
						factories.getMeasureQueryStepFactory().makeQueryStep(queryStep, measureWithUnderlyings);

				List<CubeQueryStep> underlyingSteps;
				try {
					underlyingSteps = wrappedQueryStep.getUnderlyingSteps().stream().map(underlyingStep -> {
						IMeasure notRefMeasure = resolveMeasure(underlyingStep.getMeasure());

						return CubeQueryStep.edit(underlyingStep).measure(notRefMeasure).build();
					}).toList();
				} catch (RuntimeException e) {
					String msgE = "Issue computing the underlying querySteps for %s".formatted(queryStep);
					throw AdhocExceptionHelpers.wrap(msgE, e);
				}

				registerUnderlyings(queryStep, underlyingSteps);
			} else {
				throw new UnsupportedOperationException("Issue with %s (resolved from %s)"
						.formatted(PepperLogHelper.getObjectAndClass(measure), queryStep.getMeasure()));
			}
		}
	}

	/**
	 * 
	 * @param measure
	 *            any measure
	 * @return an explicit {@link IMeasure}, hence never a {@link ReferencedMeasure}
	 */
	protected IMeasure resolveMeasure(IMeasure measure) {
		// Make sure the DAG has actual measure nodes, and not references
		IMeasure resolved = measureResolver.resolveIfRef(measure);

		// Simplify ITableMeasure into Aggregator, as ITableMeasure should not play a role in the engine
		if (resolved instanceof ITableMeasure tableMeasure && !(tableMeasure instanceof Aggregator)) {
			resolved = tableMeasure.toAggregator();
		}

		return resolved;
	}

	public static IQueryStepsDagBuilder make(IAdhocFactories factories, IQueryPod queryPod) {
		return new QueryStepsDagBuilder(factories,
				// queryPod.getTable().getName(),
				queryPod::resolveIfRef,
				queryPod.getQuery(),
				queryPod.getQueryStepCache());
	}
}

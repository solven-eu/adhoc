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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.DirectedMultigraph;
import org.jgrapht.graph.GraphCycleProhibitedException;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.model.ITableMeasure;
import eu.solven.adhoc.measure.operator.IOperatorsFactory;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps building a {@link QueryStepsDag}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class QueryStepsDagBuilder implements IQueryStepsDagBuilder {

	final IOperatorsFactory operatorsFactory;
	final String table;
	final ICubeQuery query;

	final Set<CubeQueryStep> roots = new HashSet<>();

	// The DAG maintain the actual query nodes, as it enable topological ordering
	final DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
	// The multigraph enables a queryStep to refer multiple times to the same underlying queryStep
	final DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph = new DirectedMultigraph<>(DefaultEdge.class);

	// Holds the querySteps which underlying steps are pending for processing
	// Not a Set as we want FIFO behavior, for reproducability
	final LinkedList<CubeQueryStep> pending = new LinkedList<>();

	// Holds the querySteps which underlying steps are processed
	final Set<CubeQueryStep> processed = new HashSet<>();

	public QueryStepsDagBuilder(IOperatorsFactory operatorsFactory, String cube, ICubeQuery query) {
		this.operatorsFactory = operatorsFactory;
		this.table = cube;
		this.query = query;
	}

	protected void addRoot(IMeasure queriedMeasure) {
		CubeQueryStep rootStep = CubeQueryStep.edit(query).measure(queriedMeasure).build();

		roots.add(rootStep);
		addVertex(rootStep);
		pending.add(rootStep);
	}

	protected boolean addVertex(CubeQueryStep step) {
		boolean added = dag.addVertex(step);
		boolean addedMultigraph = multigraph.addVertex(step);

		if (added != addedMultigraph) {
			throw new IllegalStateException("Inconsistent vertices around step=%s".formatted(step));
		}

		return added;
	}

	protected boolean hasLeftovers() {
		return !pending.isEmpty();
	}

	protected CubeQueryStep pollLeftover() {
		return pending.poll();
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
		} catch (GraphCycleProhibitedException e) {
			throw new IllegalStateException(
					"Issue adding `%s`->`%s` in cycle=`%s`".formatted(queriedStep, underlyingStep, dag),
					e);
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

	public QueryStepsDag getQueryDag() {
		return QueryStepsDag.builder().dag(dag).multigraph(multigraph).queried(roots).build();
	}

	@Override
	public void registerRootWithUnderlyings(ICanResolveMeasure canResolveMeasures, Set<IMeasure> queriedMeasures) {
		queriedMeasures.forEach(queriedMeasure -> {
			queriedMeasure = resolveMeasure(canResolveMeasures, queriedMeasure);

			addRoot(queriedMeasure);
		});

		// Add implicitly requested steps
		while (hasLeftovers()) {
			CubeQueryStep queryStep = pollLeftover();

			IMeasure measure = canResolveMeasures.resolveIfRef(queryStep.getMeasure());

			if (measure instanceof Aggregator aggregator) {
				log.debug("Aggregators (here {}) do not have any underlying measure", aggregator);
			} else if (measure instanceof IHasUnderlyingMeasures measureWithUnderlyings) {
				ITransformatorQueryStep wrappedQueryStep = measureWithUnderlyings.wrapNode(operatorsFactory, queryStep);

				List<CubeQueryStep> underlyingSteps;
				try {
					underlyingSteps = wrappedQueryStep.getUnderlyingSteps().stream().map(underlyingStep -> {
						IMeasure notRefMeasure = resolveMeasure(canResolveMeasures, underlyingStep.getMeasure());

						return CubeQueryStep.edit(underlyingStep).measure(notRefMeasure).build();
					}).toList();
				} catch (RuntimeException e) {
					String msgE = "Issue computing the underlying querySteps for %s".formatted(queryStep);
					if (e instanceof IllegalStateException) {
						throw new IllegalStateException(msgE, e);
					} else {
						throw new IllegalArgumentException(msgE, e);
					}
				}

				registerUnderlyings(queryStep, underlyingSteps);
			} else {
				throw new UnsupportedOperationException("Issue with %s (resolved from %s)"
						.formatted(PepperLogHelper.getObjectAndClass(measure), queryStep.getMeasure()));
			}
		}

		sanityChecks();
	}

	/**
	 * 
	 * @param canResolveMeasures
	 * @param queriedMeasure
	 *            any measure
	 * @return
	 */
	protected IMeasure resolveMeasure(ICanResolveMeasure canResolveMeasures, IMeasure measure) {
		// Make sure the DAG has actual measure nodes, and not references
		IMeasure resolved = canResolveMeasures.resolveIfRef(measure);

		// Simplify ITableMeasure into Aggregator, as ITableMeasure should not play a role in the engine
		if (resolved instanceof ITableMeasure tableMeasure && !(tableMeasure instanceof Aggregator)) {
			resolved = tableMeasure.toAggregator();
		}

		return resolved;
	}
}

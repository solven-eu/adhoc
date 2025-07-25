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
package eu.solven.adhoc.engine.observability;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import eu.solven.adhoc.engine.QueryStepsDag;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.eventbus.AdhocLogEvent.AdhocLogEventBuilder;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.transformator.IHasAggregationKey;
import eu.solven.adhoc.measure.transformator.IHasCombinationKey;
import eu.solven.adhoc.measure.transformator.IHasDecompositionKey;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.util.IAdhocEventBus;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps understanding a queryPlan for an {@link ICubeQuery}.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
public class DagExplainer implements IDagExplainer {
	private static final String FAKE_ROOT_MEASURE = "$ADHOC$fakeRoot";

	static final CubeQueryStep FAKE_ROOT = CubeQueryStep.builder()
			.measure(ReferencedMeasure.ref(FAKE_ROOT_MEASURE))
			.filter(IAdhocFilter.MATCH_ALL)
			.groupBy(IAdhocGroupBy.GRAND_TOTAL)
			.build();

	@NonNull
	IAdhocEventBus eventBus;

	/**
	 * Store the mutating state during an `EXPLAIN`, like some details about the indentation.
	 * 
	 * @author Benoit Lacelle
	 */
	@Value
	@RequiredArgsConstructor
	public static class DagExplainerState {
		final Map<CubeQueryStep, String> stepToIndentation = new HashMap<>();
		final Map<CubeQueryStep, Integer> stepToReference = new HashMap<>();

		AdhocQueryId queryId;
		QueryStepsDag dag;

		public List<CubeQueryStep> getUnderlyingSteps(CubeQueryStep step) {
			if (step == FAKE_ROOT) {
				// Requesting the underlyings of the fakeRoot is requesting the (User) queried steps
				return dag.getQueried().stream().sorted(this.orderForExplain()).toList();
			} else {
				// Return the actual underlying steps
				DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> rawDag = dag.getDag();
				return rawDag.outgoingEdgesOf(step)
						.stream()
						.map(edge -> Graphs.getOppositeVertex(rawDag, edge, step))
						.toList();
			}
		}

		/**
		 * 
		 * @return a {@link Comparator} to have deterministic and human-friendly EXPLAIN
		 */
		protected Comparator<CubeQueryStep> orderForExplain() {
			return Comparator.<CubeQueryStep, String>comparing(qr -> qr.getMeasure().toString())
					.thenComparing(qr -> qr.getFilter().toString())
					.thenComparing(qr -> qr.getGroupBy().toString());
		}

	}

	@Override
	public void explain(AdhocQueryId queryId, QueryStepsDag dag) {
		DagExplainerState state = newDagExplainerState(queryId, dag);

		printStepAndUnderlyings(state, FAKE_ROOT, Optional.empty(), true);
	}

	protected DagExplainerState newDagExplainerState(AdhocQueryId queryId, QueryStepsDag dag) {
		return new DagExplainerState(queryId, dag);
	}

	/**
	 * 
	 * @param dagState
	 * @param step
	 *            currently show queryStep
	 * @param optParent
	 * @param isLast
	 *            true if this step is the last amongst its siblings.
	 */
	protected void printStepAndUnderlyings(DagExplainerState dagState,
			CubeQueryStep step,
			Optional<CubeQueryStep> optParent,
			boolean isLast) {
		boolean isReferenced;
		{
			String parentIndentation = optParent.map(dagState.stepToIndentation::get).orElse("");

			String indentation;
			if (optParent.isEmpty()) {
				indentation = "";
			} else {
				// We keep `|` symbols as they are relevant for the next lines
				indentation = parentIndentation.replace('\\', ' ').replace('-', ' ');

				if (isLast) {
					indentation += "\\-- ";
				} else {
					indentation += "|\\- ";
				}
			}

			dagState.stepToIndentation.putIfAbsent(step, indentation);

			String stepAsString = toString(dagState, step);
			isReferenced = stepAsString.startsWith("!");

			String additionalStepInfo = additionalInfo(dagState, step, indentation, isLast, isReferenced);

			eventBus.post(openEventBuilder().message("%s%s%s".formatted(indentation, stepAsString, additionalStepInfo))
					.build());
		}

		if (!isReferenced) {
			List<CubeQueryStep> underlyings = dagState.getUnderlyingSteps(step);

			for (int i = 0; i < underlyings.size(); i++) {
				CubeQueryStep underlyingStep = underlyings.get(i);

				boolean isLastUnderlying = i == underlyings.size() - 1;
				printStepAndUnderlyings(dagState, underlyingStep, Optional.of(step), isLastUnderlying);
			}
		}
	}

	protected AdhocLogEventBuilder openEventBuilder() {
		return AdhocLogEvent.builder().explain(true).source(this);
	}

	// Typically overriden by DagExplainerForPerfs
	protected String additionalInfo(DagExplainerState dagState,
			CubeQueryStep step,
			String indentation,
			boolean isLast,
			boolean isReferenced) {
		return "";
	}

	protected String toString(DagExplainerState dagState, CubeQueryStep step) {
		Map<CubeQueryStep, Integer> stepToReference = dagState.getStepToReference();
		if (stepToReference.containsKey(step)) {
			return "!" + stepToReference.get(step);
		} else {
			int ref = stepToReference.size();
			stepToReference.put(step, ref);
			return "#" + ref + " " + toString2(dagState, step);
		}
	}

	@SuppressWarnings({ "PMD.InsufficientStringBufferDeclaration", "PMD.ConsecutiveLiteralAppends" })
	protected String toString2(DagExplainerState dagState, CubeQueryStep step) {
		if (step == FAKE_ROOT) {
			AdhocQueryId queryId = dagState.getQueryId();
			UUID parentQueryId = queryId.getParentQueryId();
			if (parentQueryId == null) {
				return "s=%s id=%s".formatted(queryId.getCube(), queryId.getQueryId());
			} else {
				return "s=%s id=%s (parentId=%s)".formatted(queryId.getCube(), queryId.getQueryId(), parentQueryId);
			}
		}

		StringBuilder sb = new StringBuilder();

		sb.append("m=")
				.append(step.getMeasure().getName())
				.append('(')
				.append(toString(step))
				.append(')')

				.append(" filter=")
				.append(step.getFilter())

				.append(" groupBy=")
				.append(step.getGroupBy());

		// Print the customMarker only if it is nonNull
		Optional<?> optCustomMarker = step.optCustomMarker();
		optCustomMarker.ifPresent(customMarker -> sb.append(" customMarker=").append(customMarker));

		return sb.toString();
	}

	protected String toString(CubeQueryStep step) {
		if (step.getMeasure() instanceof Aggregator aggregator) {
			// In the DAG EXPLAIN, the Aggregator state is implicit by being a leaf: showing the aggregationKey is much
			// more helpful
			return aggregator.getAggregationKey();
		} else {
			String string = step.getMeasure().getClass().getSimpleName();
			if (step.getMeasure() instanceof IHasDecompositionKey hasDecomposition) {
				string += "[%s]".formatted(hasDecomposition.getDecompositionKey());
			}

			if (step.getMeasure() instanceof IHasCombinationKey hasCombination) {
				string += "[%s]".formatted(hasCombination.getCombinationKey());
			}

			if (step.getMeasure() instanceof IHasAggregationKey hasAggregationKey) {
				string += "[%s]".formatted(hasAggregationKey.getAggregationKey());
			}
			return string;
		}
	}

	@Override
	public String toString() {
		// Simpler toString not to pollute the logs as source of AdhocLogEvent
		return this.getClass().getSimpleName();
	}
}

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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.IHasDagFromInducedToInducer;
import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.eventbus.AdhocLogEvent.AdhocLogEventBuilder;
import eu.solven.adhoc.eventbus.IAdhocEventBus;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.transformator.IHasAggregationKey;
import eu.solven.adhoc.measure.transformator.IHasCombinationKey;
import eu.solven.adhoc.measure.transformator.IHasDecompositionKey;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.filter.ISliceFilter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps understanding a queryPlan for an {@link ICubeQuery}. It will print in log each step as a row, from roots to
 * leaves, with ASCII-like representation of edges.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
public class DagExplainer implements IDagExplainer {
	private static final String FAKE_ROOT_MEASURE = "$ADHOC$fakeRoot";

	// Requesting the underlyings of the fakeRoot is requesting the (User) queried steps
	static final CubeQueryStep FAKE_ROOT = CubeQueryStep.builder()
			.measure(ReferencedMeasure.ref(FAKE_ROOT_MEASURE))
			.filter(ISliceFilter.MATCH_ALL)
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
		IHasDagFromInducedToInducer dag;

		public List<CubeQueryStep> getUnderlyingSteps(CubeQueryStep step) {
			if (step == FAKE_ROOT) {

				// roots are the most abstract nodes, induced by other steps
				// We show them first as they enable a nicer DAG representation
				ImmutableSet<CubeQueryStep> roots = dag.getRoots();

				// Explicit steps may be root, or not. An explicit step which is not a root should be shown in the DAG
				// for human readability (else it may be difficult to find explicit in the middle of the DAG). But we
				// show them at the end as they are less interesting.
				ImmutableSet<CubeQueryStep> explicits = dag.getExplicits();
				ImmutableSet<CubeQueryStep> explicitsNotRoot = ImmutableSet.copyOf(Sets.difference(explicits, roots));

				List<CubeQueryStep> explainerRoots = new ArrayList<>();

				explainerRoots.addAll(roots.stream().sorted(this.orderForExplain()).toList());
				explainerRoots.addAll(explicitsNotRoot.stream().sorted(this.orderForExplain()).toList());

				return explainerRoots;
			} else {
				// Return the actual underlying steps
				return dag.getInducers(step);
			}
		}

		/**
		 * 
		 * @return a {@link Comparator} to have deterministic and human-friendly EXPLAIN.
		 */
		protected Comparator<CubeQueryStep> orderForExplain() {
			return Comparator.<CubeQueryStep, String>comparing(qr -> qr.getMeasure().getName())
					.thenComparing(qr -> qr.getFilter().toString())
					.thenComparing(qr -> qr.getGroupBy().toString());
		}

	}

	@Override
	public void explain(AdhocQueryId queryId, IHasDagFromInducedToInducer dag) {
		String cubeOrTable = holderType(queryId);
		log.info("[EXPLAIN] query steps DAG on {}={} has {} inducers leading to {} induced (including {} roots)",
				cubeOrTable,
				queryId.getCube(),
				dag.getInducers().size(),
				dag.getInduceds().size(),
				dag.getRoots().size());

		DagExplainerState state = newDagExplainerState(queryId, dag);

		printStepAndUnderlyings(state, FAKE_ROOT, Optional.empty(), true);
	}

	protected DagExplainerState newDagExplainerState(AdhocQueryId queryId, IHasDagFromInducedToInducer dag) {
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
			String indentation;
			if (optParent.isEmpty()) {
				indentation = "";
			} else {
				String parentIndentation = optParent.map(dagState.stepToIndentation::get).orElse("");
				// We keep `|` symbols as they are relevant for the next lines
				indentation = parentIndentation.replace('\\', ' ').replace('-', ' ');

				if (isLast) {
					indentation += "\\-- ";
				} else {
					indentation += "|\\- ";
				}
			}

			dagState.stepToIndentation.putIfAbsent(step, indentation);

			if (indentation.isEmpty()) {
				// This is the root node: add some sort of opening indentation.
				// But it must not be registered in stepToIndentation as children should not re-apply it
				indentation = "/-- ";
			}

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

			String cubeOrTable = holderType(queryId);

			if (parentQueryId == null) {
				return "%s=%s id=%s".formatted(cubeOrTable, queryId.getCube(), queryId.getQueryId());
			} else {
				return "%s=%s id=%s (parentId=%s)"
						.formatted(cubeOrTable, queryId.getCube(), queryId.getQueryId(), parentQueryId);
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

	private String holderType(AdhocQueryId queryId) {
		String cubeOrTable;
		if (queryId.isCubeElseTable()) {
			cubeOrTable = "c";
		} else {
			cubeOrTable = "t";
		}
		return cubeOrTable;
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

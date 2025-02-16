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
package eu.solven.adhoc.dag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.util.IAdhocEventBus;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps understanding a queryPlan for an {@link IAdhocQuery}.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class DagExplainer {
	@NonNull
	IAdhocEventBus eventBus;

	public void explain(DagHolder dag) {
		Map<AdhocQueryStep, String> stepToIndentation = new HashMap<>();
		Map<AdhocQueryStep, Integer> stepToReference = new HashMap<>();

		dag.getQueried().forEach(rootStep -> {
			printStepAndUnderlyings(dag, stepToIndentation, stepToReference, rootStep, Optional.empty(), true);
		});
	}

	private void printStepAndUnderlyings(DagHolder dagHolder,
			Map<AdhocQueryStep, String> stepToIndentation,
			Map<AdhocQueryStep, Integer> stepToReference,
			AdhocQueryStep step,
			Optional<AdhocQueryStep> optParent,
			boolean isLast) {
		boolean isReferenced;
		{
			String parentIndentation = optParent.map(parentStep -> stepToIndentation.get(parentStep)).orElse("");

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

			stepToIndentation.putIfAbsent(step, indentation);

			String stepAsString = toString(stepToReference, step);

			isReferenced = stepAsString.startsWith("!");

			eventBus.post(AdhocLogEvent.builder()
					.explain(true)
					.message("%s%s".formatted(indentation, stepAsString))
					.source(this)
					.build());
		}

		if (!isReferenced) {

			DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> dag = dagHolder.getDag();
			List<DefaultEdge> underlyings = dag.outgoingEdgesOf(step).stream().toList();

			for (int i = 0; i < underlyings.size(); i++) {
				DefaultEdge edge = underlyings.get(i);
				AdhocQueryStep underlyingStep = Graphs.getOppositeVertex(dag, edge, step);

				printStepAndUnderlyings(dagHolder,
						stepToIndentation,
						stepToReference,
						underlyingStep,
						Optional.of(step),
						i == underlyings.size() - 1);

			}
		}
	}

	protected String toString(Map<AdhocQueryStep, Integer> stepToReference, AdhocQueryStep step) {
		if (stepToReference.containsKey(step)) {
			return "!" + stepToReference.get(step);
		} else {
			int ref = stepToReference.size();
			stepToReference.put(step, ref);
			return "#" + ref + " " + toString(step);
		}
	}

	private String toString(AdhocQueryStep step) {
		StringBuilder sb = new StringBuilder();

		sb.append("m=")
				.append(step.getMeasure().getName())
				.append("(")
				.append(step.getMeasure().getClass().getSimpleName())
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

}

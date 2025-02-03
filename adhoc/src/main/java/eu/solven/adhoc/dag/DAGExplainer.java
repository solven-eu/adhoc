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
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Builder
@Slf4j
public class DAGExplainer {
	@NonNull
	IAdhocEventBus eventBus;

	public void explain(DagHolder dag) {
		Map<AdhocQueryStep, String> stepToIndentation = new HashMap<>();
		// Map<AdhocQueryStep, Integer> stepToReference = new HashMap<>();

		dag.getQueried().forEach(rootStep -> {
			printStepAndUndelryings(dag, stepToIndentation, rootStep, Optional.empty(), true);
		});
	}

	private void printStepAndUndelryings(DagHolder dagHolder,
			Map<AdhocQueryStep, String> stepToIndentation,
			AdhocQueryStep step,
			Optional<AdhocQueryStep> optParent,
			boolean isLast) {

		if (stepToIndentation.containsKey(step)) {
			log.trace("This event has already been processed: {}", step);
		} else {
			String parentIndentation = optParent.map(parentStep -> stepToIndentation.get(parentStep)).orElse("");
			// stepToIndentation.putIfAbsent(step, stepToIndentation.size());

			String indentation;
			if (optParent.isEmpty()) {
				indentation = "";
			} else {
				// We keep `|` symbols as they are relevant for the next lines
				indentation = parentIndentation.replace('\\', ' ').replace('-', ' ');

				// if (!parentIndentation.isEmpty()) {
				if (isLast) {
					indentation += "\\-- ";
				} else {
					indentation += "|\\- ";
				}
				// }
			}

			stepToIndentation.putIfAbsent(step, indentation);

			eventBus.post(AdhocLogEvent.builder()
					.explain(true)
					.message("%s%s".formatted(indentation, toString(step)))
					.source(this)
					.build());

			DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> dag = dagHolder.getDag();
			List<DefaultEdge> underlyings = dag.outgoingEdgesOf(step).stream().toList();

			for (int i = 0; i < underlyings.size(); i++) {
				DefaultEdge edge = underlyings.get(i);
				AdhocQueryStep underlyingStep = Graphs.getOppositeVertex(dag, edge, step);

				printStepAndUndelryings(dagHolder,
						stepToIndentation,
						underlyingStep,
						Optional.of(step),
						i == underlyings.size() - 1);

			}
		}
	}

	protected String toString(AdhocQueryStep step) {
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

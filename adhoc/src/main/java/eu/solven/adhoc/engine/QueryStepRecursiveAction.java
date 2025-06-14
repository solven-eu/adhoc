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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.function.Consumer;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.base.MoreObjects;

import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Wraps a {@link QueryStepsDag} step into a {@link RecursiveAction}, to be submitted into a {@link ForkJoinPool}.
 * 
 * @author Benoit Lacelle
 */
@Builder(toBuilder = true)
@Slf4j
// BEWARE Adhoc does not enable remote execution of such task. Please fill a ticket if you believe it would be
// beneficial.
@SuppressWarnings("PMD.NonSerializableClass")
public class QueryStepRecursiveAction extends RecursiveAction {
	private static final long serialVersionUID = -8742698545892380483L;

	// BEWARE This should be immutable/not-mutated during the course of the related actions
	@NonNull
	DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> fromAggregatesToQueried;

	@NonNull
	CubeQueryStep step;

	// BEWARE This should be a thread-safe Map!
	@NonNull
	Map<CubeQueryStep, ISliceToValue> queryStepToValues;

	// Similar to `Spliterator`: the action is delegated to some Consumer
	@NonNull
	Consumer<? super CubeQueryStep> onReadyStep;

	@Override
	protected void compute() {
		Set<DefaultEdge> outgoingEdgesOf = fromAggregatesToQueried.outgoingEdgesOf(step);
		List<CubeQueryStep> missingSteps = outgoingEdgesOf.stream()
				.map(fromAggregatesToQueried::getEdgeTarget)
				.filter(s -> !queryStepToValues.containsKey(s))
				.toList();

		if (!missingSteps.isEmpty()) {
			// In general, all underlyings are missing. But we may imagine some tasks to be already computed for any
			// reason (e.g. cache).
			List<QueryStepRecursiveAction> missingTasks =
					missingSteps.stream().map(missingStep -> this.toBuilder().step(missingStep).build()).toList();

			// BEWARE This generates stacks as deep as the DAG: `-Xss4M`
			log.debug("Invoking missing underlyings for step={} missing_steps={}",
					step.getId(),
					missingSteps.stream().map(CubeQueryStep::getId).toList());
			invokeAll(missingTasks);
		}
		log.debug("Executing step: {}", step.getId());
		onReadyStep.accept(step);
	}

	@Override
	public String toString() {
		int vertexSize = fromAggregatesToQueried.vertexSet().size();
		int ready = queryStepToValues.size();
		return MoreObjects.toStringHelper(this)
				.add("step", step)
				.add("#ready", ready)
				.add("#missing", vertexSize - ready)
				.toString();
	}
}

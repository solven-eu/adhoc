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
package eu.solven.adhoc.engine.concurrent;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.function.Consumer;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.base.MoreObjects;

import eu.solven.adhoc.engine.QueryStepsDag;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.With;
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
public class DagRecursiveAction<T> extends RecursiveAction {
	private static final long serialVersionUID = -8742698545892380483L;

	// BEWARE This should be immutable/not-mutated during the course of the related actions
	@NonNull
	final DirectedAcyclicGraph<T, DefaultEdge> fromQueriedToDependencies;

	@NonNull
	@With
	final T step;

	@NonNull
	@Default
	final ConcurrentMap<T, DagRecursiveAction<T>> stepToTask = new ConcurrentHashMap<>();

	// BEWARE This should be a thread-safe Map!
	// BEWARE This must not be an ImmutableMap, as this is modified to receive the results
	@NonNull
	final Map<T, ?> queryStepToValues;

	// Similar to `Spliterator`: the action is delegated to some Consumer
	@NonNull
	final Consumer<? super T> onReadyStep;

	@Override
	protected void compute() {
		Set<DefaultEdge> outgoingEdgesOf = fromQueriedToDependencies.outgoingEdgesOf(step);
		List<T> missingSteps = outgoingEdgesOf.stream()
				.map(fromQueriedToDependencies::getEdgeTarget)
				// Some tasks may be already done (e.g. cache, task already completed from another parent)
				.filter(s -> !queryStepToValues.containsKey(s))
				.toList();

		List<DagRecursiveAction<T>> missingStepStasks =
				// Ensure we have a single task per step, to enable sharing
				missingSteps.stream().map(d -> stepToTask.computeIfAbsent(d, missingStep -> {
					DagRecursiveAction<T> missingStepAction = this.toBuilder().step(missingStep).build();

					// Fork only once, as ForkJoinTasks must not be forked multiple times (without reinitialization)
					missingStepAction.fork();
					log.debug("Forked task for step={}", missingStep);

					return missingStepAction;
				})).toList();

		// Join results
		missingStepStasks.forEach(ForkJoinTask::join);

		log.debug("Executing step: {} after having waiting for {} (amongst {})",
				step,
				missingStepStasks.size(),
				outgoingEdgesOf.size());
		onReadyStep.accept(step);
	}

	@Override
	public String toString() {
		int vertexSize = fromQueriedToDependencies.vertexSet().size();
		int ready = queryStepToValues.size();
		return MoreObjects.toStringHelper(this)
				.add("step", step)
				.add("#ready", ready)
				.add("#missing", vertexSize - ready)
				.toString();
	}
}

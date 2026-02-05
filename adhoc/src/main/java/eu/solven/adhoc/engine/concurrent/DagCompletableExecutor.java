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
package eu.solven.adhoc.engine.concurrent;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.base.MoreObjects;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Enables processing a {@link DirectedAcyclicGraph}, with shared nodes, through a CompletionFuture.
 * 
 * @param <T>
 * @author Benoit Lacelle
 */
@Slf4j
@Builder
public class DagCompletableExecutor<T> {

	// BEWARE This should be immutable/not-mutated during the course of the related actions
	@NonNull
	final DirectedAcyclicGraph<T, DefaultEdge> fromQueriedToDependencies;

	// @NonNull
	// @With
	// final T step;

	@NonNull
	@Default
	final ConcurrentMap<T, WiringFuture> stepToWiring = new ConcurrentHashMap<>();

	@NonNull
	@Default
	final ConcurrentMap<T, CompletableFuture<Void>> stepToFuture = new ConcurrentHashMap<>();

	// BEWARE This should be a thread-safe Set, typically as view of a Map receiving results in onReadyStep !
	@NonNull
	final Set<T> queryStepsDone;

	// Similar to `Spliterator`: the action is delegated to some Consumer
	@NonNull
	final Consumer<? super T> onReadyStep;

	@NonNull
	final Executor executor;

	public static class WiringFuture extends CompletableFuture<Void> {
		final AtomicBoolean wired = new AtomicBoolean();
	}

	public CompletableFuture<Void> executeRecursively(T step) {
		// Short-circuit: already computed (cache hit)
		if (queryStepsDone.contains(step)) {
			return CompletableFuture.completedFuture(null);
		}

		// Phase 1: publish placeholder, to prevent recursive update in `computeIfAbsent`
		// Wiring may be just created, on-going in different thread, or already completed
		WiringFuture wiring = stepToWiring.computeIfAbsent(step, s -> new WiringFuture());

		// Phase 2: only one thread wires it
		if (wiring.wired.compareAndSet(false, true)) {
			// We are the one wiring
			wire(step, wiring);

			// After `.wire`, we're guaranteed to have a ready future
			return stepToFuture.get(step);
		} else {
			// BEWARE Another thread might be preparing the future, but it may not be registered in stepToFuture
			// It may wired, or being wired in another thread
			return wiring.thenCompose(a -> stepToFuture.get(step));
		}

	}

	public CompletableFuture<Void> executeRecursively(List<T> steps) {
		List<CompletableFuture<Void>> futures = steps.stream().map(step -> executeRecursively(step)).toList();

		return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
	}

	protected void wire(T step, WiringFuture wiring) {
		try {

			Set<DefaultEdge> outgoingEdges = fromQueriedToDependencies.outgoingEdgesOf(step);

			List<CompletableFuture<Void>> dependencyFutures = outgoingEdges.stream()
					.map(fromQueriedToDependencies::getEdgeTarget)
					.filter(missingStep -> !queryStepsDone.contains(missingStep))
					// safe: placeholder already visible
					// recursive, but stack-safe
					.map(this::executeRecursively)
					.toList();

			CompletableFuture<Void> missingDependencies =
					CompletableFuture.allOf(dependencyFutures.toArray(CompletableFuture[]::new));

			CompletableFuture<Void> wiredFuture = missingDependencies.thenRunAsync(() -> {
				log.debug("Executing step: {} after waiting for {} (amongst {})",
						step,
						dependencyFutures.size(),
						outgoingEdges.size());
				onReadyStep.accept(step);
			}, executor);

			stepToFuture.put(step, wiredFuture);
			wiring.complete(null);
		} catch (Throwable t) {
			wiring.completeExceptionally(t);
			throw t;
		}
	}

	@Override
	public String toString() {
		int vertexSize = fromQueriedToDependencies.vertexSet().size();
		int ready = queryStepsDone.size();
		return MoreObjects.toStringHelper(this)
				// .add("step", step)
				.add("#ready", ready)
				.add("#missing", vertexSize - ready)
				.toString();
	}
}

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

import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link Spliterator} similar to {@link TopologicalOrderIterator}. It enables {@link Stream}, including with
 * `.parallel()`.
 * 
 * BEWARE This does not enable concurrency as `.trySplit()` can not be called on a per-task basis. Hence, if the graph
 * has a single original queryStep (which is often the case, as a single `.Aggregator` step not doing much), then we
 * have no later occasion to fork more threads when the graph of steps widens..
 * 
 * @author Benoit Lacelle
 */
@Deprecated(since = "Irrelevant as Spliterator.trySplit() are only called early")
@Slf4j
public class TopologicalOrderSpliterator implements Spliterator<CubeQueryStep> {
	final EdgeReversedGraph<CubeQueryStep, DefaultEdge> fromAggregatesToQueried;
	final ImmutableList<CubeQueryStep> topologicalOrderList;
	final int size;

	final Set<CubeQueryStep> processed = Sets.newConcurrentHashSet();
	final AtomicInteger nbSplit = new AtomicInteger(0);
	final AtomicInteger indexNext = new AtomicInteger(0);
	final WriteLock writeLock = new ReentrantReadWriteLock().writeLock();

	public TopologicalOrderSpliterator(EdgeReversedGraph<CubeQueryStep, DefaultEdge> fromAggregatesToQueried) {
		this.fromAggregatesToQueried = fromAggregatesToQueried;

		// https://en.wikipedia.org/wiki/Topological_sorting
		// TopologicalOrder guarantees processing a vertex after dependent vertices are
		// done.
		this.topologicalOrderList = ImmutableList.copyOf(new TopologicalOrderIterator<>(fromAggregatesToQueried));

		this.size = topologicalOrderList.size();
	}

	@Override
	public boolean tryAdvance(Consumer<? super CubeQueryStep> action) {
		int indexBeingProcessed;
		CubeQueryStep next;

		writeLock.lock();
		try {
			// Do not increment yet, as this task may actually not be available
			// Typically, on `{A, B} -> C`, once `A` done, `tryAdvance` should not do anything as we nee to wait
			// for `B`
			indexBeingProcessed = indexNext.get();

			if (indexBeingProcessed >= size) {
				// Happens when multiple trySplit occurred and we reach the end of the iterator
				log.debug("Closing a forked spliterator due to completion");
				return false;
			}

			next = topologicalOrderList.get(indexBeingProcessed);

			boolean allProcessed = canBeProcessed(next);
			if (allProcessed) {
				// Next tryAdvance has to consider next element
				indexNext.incrementAndGet();
			} else {
				// This will close the spliterator: fine, as we typically needs to reduce our concurrency
				// Happens typically on the last measure: its underlyings can be processed in parallel while itself can
				// be processed by a single thread.
				log.debug("Closing a forked spliterator due to diminished parallelism (stepIndex={})",
						indexBeingProcessed);
				return false;
			}

		} finally {
			writeLock.unlock();
		}

		log.debug("tryAdvance {} / {}", indexBeingProcessed, topologicalOrderList.size());
		action.accept(next);
		processed.add(next);
		log.debug("tryAdvance {} / {} after", indexBeingProcessed, topologicalOrderList.size());

		return estimateSize() > 0;
	}

	protected boolean canBeProcessed(CubeQueryStep next) {
		Set<DefaultEdge> incomingEdgesOf = fromAggregatesToQueried.incomingEdgesOf(next);
		return incomingEdgesOf.stream().map(fromAggregatesToQueried::getEdgeSource).allMatch(processed::contains);
	}

	// We should split at most N, where N is the maximum number of concurrent tasks
	// `trySplit` is called even before the first `tryAdvance`. We should split if we detect the number of
	// spliterators is lower than the available tasks (at current state)
	@Override
	public Spliterator<CubeQueryStep> trySplit() {
		// Count how many split would be useful at current state
		int nbAvailableTasks = 0;

		writeLock.lock();
		try {
			int nextToProcess = indexNext.get();
			for (int indexBeingProcessed = nextToProcess; indexBeingProcessed < size; indexBeingProcessed++) {
				CubeQueryStep next = topologicalOrderList.get(indexBeingProcessed);
				if (canBeProcessed(next)) {
					nbAvailableTasks++;
				} else {
					// Next steps can necessarily not be processed
					break;
				}
			}
		} finally {
			writeLock.unlock();
		}

		// if `nbSplit==0`, it means we have a single (current) thread
		if (nbAvailableTasks > nbSplit.get() + 1) {
			// This Spliterator is stateless: we can return another reference of it
			// This will lead to a second thread processing whatever next queryStep is available
			int nextSplit = nbSplit.incrementAndGet();
			log.debug("trySplit accepted, to nextSplit={} given maxSplit={}", nextSplit, nbAvailableTasks);
			return this;
		}

		log.debug("trySplit refused, kept at nbSplit={}", nbSplit.get());
		return null;
	}

	@Override
	public long estimateSize() {
		long problemSize = fromAggregatesToQueried.vertexSet().size();
		return problemSize - processed.size();
	}

	@Override
	public int characteristics() {
		// Not sub-sized as on trySplit, the bunch of forked querySteps can not be sized as it depends on the
		// execution of other nodes
		// Not ordered as some nextStep would depends on query execution order
		return SIZED | DISTINCT | IMMUTABLE | NONNULL;
	}

	protected static Spliterator<CubeQueryStep> makeSpliterator(DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag) {
		// https://stackoverflow.com/questions/69183360/traversal-of-edgereversedgraph
		EdgeReversedGraph<CubeQueryStep, DefaultEdge> fromAggregatesToQueried = new EdgeReversedGraph<>(dag);

		return new TopologicalOrderSpliterator(fromAggregatesToQueried);
	}

	public static Stream<CubeQueryStep> fromDAG(DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag) {
		Spliterator<CubeQueryStep> spliterator = makeSpliterator(dag);

		// By default, the engine is mono-threaded.
		// `TopologicalOrderSpliterator` can be turned into an effective parallel Stream, but we do not want to fork
		// into some ForkJoinPool by default.
		boolean isParallel = false;

		return StreamSupport.stream(spliterator, isParallel);
	}

}
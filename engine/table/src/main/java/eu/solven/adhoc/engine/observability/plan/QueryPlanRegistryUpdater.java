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
package eu.solven.adhoc.engine.observability.plan;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import eu.solven.adhoc.engine.observability.plan.events.IQueryPlanEvent;
import eu.solven.adhoc.engine.observability.plan.events.QueryPlanCompleted;
import eu.solven.adhoc.engine.observability.plan.events.QueryPlanFailed;
import eu.solven.adhoc.engine.observability.plan.events.QueryPlanNodeCompleted;
import eu.solven.adhoc.engine.observability.plan.events.QueryPlanNodeFailed;
import eu.solven.adhoc.engine.observability.plan.events.QueryPlanNodeStarted;
import eu.solven.adhoc.engine.observability.plan.events.QueryPlanRegistered;
import eu.solven.adhoc.query.AdhocQueryId;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Applies {@link IQueryPlanEvent}s to an {@link IQueryPlanRegistry}, mutating the per-node state + stats in place.
 *
 * <p>
 * In PR 2 this updater is the only producer of registry mutations after the initial {@code register}. In PR 3 the
 * existing {@code DagExplainer} / {@code DagExplainerForPerfs} will be rewired to fire these events through the shared
 * {@code IAdhocEventBus}, with this updater as one of the subscribers.
 *
 * <p>
 * Event dispatch is via {@link #on(IQueryPlanEvent)} — a small {@code instanceof} ladder that's friendlier to call
 * sites than a {@code visitor} pattern given the event count (~6).
 *
 * @author Benoit Lacelle
 */
@Deprecated(since = "Push will be removed")
@RequiredArgsConstructor
@Slf4j
public class QueryPlanRegistryUpdater {

	protected final IQueryPlanRegistry registry;

	/**
	 * Dispatch entry point. Unknown event types are logged at debug and ignored so a future event type addition does
	 * not have to be a coordinated rollout.
	 *
	 * @param event
	 *            one of the {@link IQueryPlanEvent} concrete types
	 */
	public void on(IQueryPlanEvent event) {
		Objects.requireNonNull(event, "event");
		if (event instanceof QueryPlanRegistered registered) {
			onRegistered(registered);
		} else if (event instanceof QueryPlanNodeStarted started) {
			onNodeStarted(started);
		} else if (event instanceof QueryPlanNodeCompleted completed) {
			onNodeCompleted(completed);
		} else if (event instanceof QueryPlanNodeFailed failed) {
			onNodeFailed(failed);
		} else if (event instanceof QueryPlanCompleted completed) {
			onPlanCompleted(completed);
		} else if (event instanceof QueryPlanFailed failed) {
			onPlanFailed(failed);
		} else {
			log.debug("Unhandled IQueryPlanEvent type {} — ignoring", event.getClass().getName());
		}
	}

	protected void onRegistered(QueryPlanRegistered event) {
		registry.register(event.getPlan());
	}

	protected void onNodeStarted(QueryPlanNodeStarted event) {
		findNode(event.getQueryId(), event.getSubject()).ifPresent(node -> {
			node.setState(NodeState.RUNNING);
			node.setStats(NodeStats.builder().startedAt(event.getAt()).build());
			// Plan transitions to RUNNING on the first node that starts. Idempotent — a later started event
			// still finds state==RUNNING, no-op.
			registry.get(event.getQueryId()).ifPresent(plan -> {
				if (plan.getState() == PlanState.PENDING) {
					plan.setState(PlanState.RUNNING);
				}
			});
		});
	}

	protected void onNodeCompleted(QueryPlanNodeCompleted event) {
		findNode(event.getQueryId(), event.getSubject()).ifPresent(node -> {
			NodeStats prior = node.getStats();
			long elapsed = elapsed(prior.getStartedAt() == null ? event.getAt() : prior.getStartedAt(), event.getAt());
			node.setState(NodeState.DONE);
			node.setStats(prior.toBuilder()
					.completedAt(event.getAt())
					.elapsedMs(elapsed)
					.rowsIn(event.getRowsIn())
					.rowsOut(event.getRowsOut())
					.build());
		});
	}

	protected void onNodeFailed(QueryPlanNodeFailed event) {
		findNode(event.getQueryId(), event.getSubject()).ifPresent(node -> {
			NodeStats prior = node.getStats();
			long elapsed = elapsed(prior.getStartedAt() == null ? event.getAt() : prior.getStartedAt(), event.getAt());
			node.setState(NodeState.FAILED);
			node.setStats(prior.toBuilder()
					.completedAt(event.getAt())
					.elapsedMs(elapsed)
					.errorMessage(event.getErrorMessage())
					.build());
		});
	}

	protected void onPlanCompleted(QueryPlanCompleted event) {
		registry.get(event.getQueryId()).ifPresent(plan -> {
			plan.setState(PlanState.DONE);
			plan.setCompletedAt(event.getAt());
			// Trigger eviction in case this plan moving to a terminal state put the registry over budget — handled
			// implicitly the next time a register() is called, but doing it now keeps the budget tight even in the
			// no-new-queries-coming-soon case. The registry's register() is idempotent on the same plan instance.
			registry.register(plan);
		});
	}

	protected void onPlanFailed(QueryPlanFailed event) {
		registry.get(event.getQueryId()).ifPresent(plan -> {
			plan.setState(PlanState.FAILED);
			plan.setCompletedAt(event.getAt());
			registry.register(plan);
		});
	}

	/**
	 * Locate a node by walking the plan's DAG. Uses a visited set to handle the dedup case where a node has multiple
	 * parents. Returns the first match by {@code subject.equals(...)} — for our DAG, that's a single node, since
	 * subjects are unique-per-plan by construction.
	 *
	 * <p>
	 * Cost is O(N) per event where N is the plan's node count. For the 20k-node plans the user reports, that's still a
	 * single-digit-millisecond cost per event on modern hardware. If profiling shows this dominates, we can attach a
	 * {@code Map<Object, QueryPlanNode>} index to {@link QueryPlan} as a follow-up — the public API stays the same.
	 */
	protected Optional<QueryPlanNode> findNode(AdhocQueryId queryId, Object subject) {
		Optional<QueryPlan> plan = registry.get(queryId);
		if (plan.isEmpty()) {
			log.debug("No plan registered for queryId={} — dropping event", queryId);
			return Optional.empty();
		}
		Deque<QueryPlanNode> stack = new ArrayDeque<>();
		Set<Object> visited = new LinkedHashSet<>();
		stack.push(plan.get().getRoot());
		while (!stack.isEmpty()) {
			QueryPlanNode node = stack.pop();
			if (!visited.add(node.getSubject())) {
				continue;
			}
			if (subject.equals(node.getSubject())) {
				return Optional.of(node);
			}
			for (QueryPlanNode child : node.getChildren()) {
				stack.push(child);
			}
		}
		log.debug("No node found for subject={} in plan queryId={}", subject, queryId);
		return Optional.empty();
	}

	protected static long elapsed(java.time.Instant from, java.time.Instant to) {
		return Math.max(0L, Duration.between(from, to).toMillis());
	}
}

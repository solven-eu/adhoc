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

import java.util.Collections;
import java.util.Map;

/**
 * Write-only side of {@link IQueryPlanRegistry#publishFragment} pre-bound to a specific {@code queryId}. Extensions
 * (calculated columns, combinators, …) reach an instance via {@link PlanFragmentScope#current()} and add detail nodes
 * to the live plan without having to learn the registry / queryId pair themselves.
 *
 * <p>
 * Default helpers cover the two common shapes:
 * <ul>
 * <li>{@link #publishLeaf} — a free-form leaf carrying {@code details} (label + Map). Use this for "I want the plan to
 * show what I'm doing right now", e.g. the rendered SQL of a calculated column, the marker chosen by a routing measure,
 * the buffer-strategy a combinator picked. The {@code leafKey} controls dedup — re-publishing with the same key
 * replaces the previous fragment rather than appending.</li>
 * <li>{@link #publish} — full control over the published {@link QueryPlanNode} for callers that want to attach a
 * sub-tree rather than a single leaf.</li>
 * </ul>
 *
 * <p>
 * Thread-safety: the runtime sink (the one created by the engine and exposed via {@link PlanFragmentScope}) routes to
 * {@link LiveQueryPlanSource#publishFragment} which is a {@code ConcurrentHashMap} write. Several extensions publishing
 * in parallel from worker threads is supported.
 *
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IPlanFragmentSink {

	/**
	 * Drop-everything sink. Returned by {@link PlanFragmentScope#current()} when no scope is bound — extensions can
	 * always {@code publish(...)} unconditionally without checking for null.
	 */
	IPlanFragmentSink NOOP = (anchor, subtree) -> {
		// drop
	};

	/**
	 * Publish {@code subtree} as an additional child of every plan node whose {@code subject.equals(anchor)}.
	 * Idempotent on {@code (anchor, subtree.subject)} — see {@link LiveQueryPlanSource#publishFragment}.
	 *
	 * @param anchor
	 *            the subject value the projector matches against existing nodes (typically the {@code CubeQueryStep}
	 *            the extension is contributing to)
	 * @param subtree
	 *            the fragment to graft
	 */
	void publish(Object anchor, QueryPlanNode subtree);

	/**
	 * Convenience for the common "publish one leaf" case. Builds a {@link QueryPlanNode} with the provided fields and
	 * routes through {@link #publish}.
	 *
	 * @param anchor
	 *            the anchor subject — typically the {@code CubeQueryStep} the extension is wired under
	 * @param leafKey
	 *            stable identity for the leaf so a re-published fragment replaces rather than appends. Must implement
	 *            {@code equals}/{@code hashCode}; a small record is the canonical shape
	 * @param operator
	 *            the {@link NodeOperator} (typically {@link NodeOperator#OTHER} for extension-defined nodes)
	 * @param label
	 *            human-readable label rendered in the plan view
	 * @param details
	 *            free-form key→value details; {@link Map#of()} is acceptable for empty
	 */
	default void publishLeaf(Object anchor,
			Object leafKey,
			NodeOperator operator,
			String label,
			Map<String, String> details) {
		publish(anchor,
				QueryPlanNode.builder()
						.subject(leafKey)
						.operator(operator)
						.label(label)
						.state(NodeState.DONE)
						.details(details == null ? Collections.emptyMap() : details)
						.build());
	}
}

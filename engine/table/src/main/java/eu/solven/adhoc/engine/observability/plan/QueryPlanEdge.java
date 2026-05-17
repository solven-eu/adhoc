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

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * One directed edge in a {@link QueryPlan}'s graph: a parent {@link QueryPlanNode} depends on a child
 * {@link QueryPlanNode}. Endpoints are referenced by {@link QueryPlanNode#getId() id} rather than by reference, so the
 * wire shape (and the in-memory plan) is a flat list of nodes + a flat list of edges — the projector's DAG memoization
 * survives JSON round-trip without exploding into a tree.
 *
 * <p>
 * Multi-incoming edges are normal (DAG): a single {@code TableQueryV4} served by N induced steps appears once in
 * {@link QueryPlan#getNodes()} with N edges pointing at it. Self-edges are not produced by the projector and would be a
 * publisher bug.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
@Jacksonized
public class QueryPlanEdge {
	/** Id of the parent node — the consumer of the child's output. */
	@NonNull
	String parentId;

	/** Id of the child node — the step the parent depends on. */
	@NonNull
	String childId;
}

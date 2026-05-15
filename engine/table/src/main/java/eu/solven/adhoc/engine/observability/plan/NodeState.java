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

/**
 * Execution state of a single {@link QueryPlanNode} inside a {@link QueryPlan}.
 *
 * <p>
 * The state machine is:
 *
 * <pre>
 *   PENDING --&gt; RUNNING --&gt; DONE
 *                       \--&gt; FAILED
 *   PENDING --&gt; SKIPPED   (e.g. an EXPLAIN_V2-only run never executes any node)
 * </pre>
 *
 * @author Benoit Lacelle
 */
public enum NodeState {
	/** Node is in the plan tree but execution has not started. */
	PENDING,
	/** Node is currently executing. Live View reads {@code stats.elapsedMs} as {@code now − startedAt}. */
	RUNNING,
	/** Node finished successfully; {@code stats} is final. */
	DONE,
	/** Node failed; {@code stats.errorMessage} carries the reason. */
	FAILED,
	/** Node was not executed at all — typical of EXPLAIN_V2 mode where the engine stops after plan-build. */
	SKIPPED;
}

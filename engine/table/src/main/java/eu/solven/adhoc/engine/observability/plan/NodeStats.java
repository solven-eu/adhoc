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

import java.time.Instant;

import org.jspecify.annotations.Nullable;

import lombok.Builder;
import lombok.Value;

/**
 * Per-{@link QueryPlanNode} runtime statistics. Empty in EXPLAIN_V2 mode (plan-only). Populated incrementally during
 * execution in EXPLAIN_ANALYZE_V2 / live-view mode.
 *
 * <p>
 * Memory / I/O byte counters are deliberately omitted — they are hard to measure cheaply on the JVM and we would rather
 * ship without them than ship inaccurate numbers. Add when there is a reliable mechanism.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder(toBuilder = true)
public class NodeStats {
	/** When the node transitioned from {@link NodeState#PENDING} to {@link NodeState#RUNNING}. */
	@Nullable
	Instant startedAt;

	/** When the node reached a terminal state ({@link NodeState#DONE} / {@link NodeState#FAILED}). */
	@Nullable
	Instant completedAt;

	/**
	 * Wall-clock duration in milliseconds. Live-updated while {@link NodeState#RUNNING} ({@code now − startedAt});
	 * frozen on terminal transitions to {@code completedAt − startedAt}. Clamped to be non-negative.
	 */
	long elapsedMs;

	/**
	 * Rows read by this node, when known. {@code null} when the node has no notion of input rows (e.g. measure refs).
	 */
	@Nullable
	Long rowsIn;

	/** Rows produced by this node, when known. */
	@Nullable
	Long rowsOut;

	/**
	 * Reserved for nodes whose stats are PROJECTED rather than measured (e.g. join-pruning decisions in EXPLAIN_V2
	 * before any execution has happened). {@code true} when the values above are best-effort plan-time guesses;
	 * {@code false} (default) means values come from actual execution. UI surfaces this with a small "estimated" badge.
	 */
	@Builder.Default
	boolean estimated = false;

	/** Short error string when the node FAILED. {@code null} otherwise. */
	@Nullable
	String errorMessage;

	/** Empty stats — the initial value for a node that has not started yet. */
	public static NodeStats empty() {
		return NodeStats.builder().build();
	}
}

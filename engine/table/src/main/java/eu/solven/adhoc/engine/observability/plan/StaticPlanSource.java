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

import eu.solven.adhoc.query.AdhocQueryId;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Holds a single {@link QueryPlan} instance and returns it on every {@link #snapshot()}. Used by the push-driven
 * registry path (see {@code QueryPlanRegistryUpdater}): the plan is mutated in place by the event subscriber, and
 * snapshot just hands it back — the registry takes care of deep-copying on the public read.
 *
 * <p>
 * {@link #isCompleted()} reflects the wrapped plan's current state, so the registry's eviction policy sees the
 * push-side state transitions even though no source-level activity happened.
 *
 * @author Benoit Lacelle
 */
@Deprecated(since = "Push will be removed")
@RequiredArgsConstructor
public class StaticPlanSource implements IPlanSource {
	/**
	 * The wrapped plan. Exposed (vs purely held) so the push-side {@code QueryPlanRegistryUpdater} can locate it via
	 * {@code IQueryPlanRegistry.get(id)} and mutate it in place without first paying for a deep copy.
	 */
	@Getter
	private final QueryPlan plan;

	@Override
	public AdhocQueryId getQueryId() {
		return plan.getQueryId();
	}

	@Override
	public QueryPlan snapshot() {
		return plan;
	}

	@Override
	public boolean isCompleted() {
		return plan.getState() == PlanState.DONE || plan.getState() == PlanState.FAILED;
	}
}

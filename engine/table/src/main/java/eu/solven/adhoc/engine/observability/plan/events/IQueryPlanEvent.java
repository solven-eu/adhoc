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
package eu.solven.adhoc.engine.observability.plan.events;

import java.time.Instant;

import eu.solven.adhoc.eventbus.IAdhocEvent;
import eu.solven.adhoc.query.AdhocQueryId;

/**
 * Marker interface for events that mutate a {@code QueryPlan} in the registry. Implementations are immutable value
 * objects. The intended consumer is {@code QueryPlanRegistryUpdater}, but other subscribers can listen too (e.g. an SSE
 * bridge serializing events to a UI client).
 *
 * @author Benoit Lacelle
 */
@Deprecated(since = "Push will be removed")
public interface IQueryPlanEvent extends IAdhocEvent {
	/**
	 * @return the {@link AdhocQueryId} of the plan this event targets.
	 */
	AdhocQueryId getQueryId();

	/**
	 * @return when the event was produced — used by the updater to fill {@code stats.startedAt} /
	 *         {@code stats.completedAt} without having to call {@code Instant.now()} on the listener side.
	 */
	Instant getAt();
}

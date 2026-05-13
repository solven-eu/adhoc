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

import org.jspecify.annotations.Nullable;

import eu.solven.adhoc.query.AdhocQueryId;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.With;

/**
 * A {@link eu.solven.adhoc.engine.observability.plan.QueryPlanNode} reached {@code DONE}. Carries the row counts so the
 * registry updater can fill {@link eu.solven.adhoc.engine.observability.plan.NodeStats} without a second round-trip.
 *
 * @author Benoit Lacelle
 */
@Deprecated(since = "Push will be removed")
@Value
@Builder
public class QueryPlanNodeCompleted implements IQueryPlanEvent {
	@NonNull
	AdhocQueryId queryId;

	@NonNull
	Object subject;

	@NonNull
	Instant at;

	/** Source class fqdn for SLF4J logging — see {@link eu.solven.adhoc.eventbus.IAdhocEvent}. */
	@With
	String fqdn;

	/** Rows read by this node, when known. {@code null} when the producer cannot measure it cheaply. */
	@Nullable
	Long rowsIn;

	/** Rows produced by this node, when known. */
	@Nullable
	Long rowsOut;
}

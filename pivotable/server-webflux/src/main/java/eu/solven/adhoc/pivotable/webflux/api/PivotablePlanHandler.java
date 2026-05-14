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
package eu.solven.adhoc.pivotable.webflux.api;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.engine.observability.plan.IQueryPlanRegistry;
import eu.solven.adhoc.engine.observability.plan.QueryPlan;
import eu.solven.adhoc.engine.observability.plan.QueryPlanSummary;
import eu.solven.adhoc.pivotable.query.AsynchronousStatus;
import eu.solven.adhoc.pivotable.query.PivotableAsynchronousQueriesManager;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

/**
 * WebFlux mirror of {@code PivotablePlanController}. Implements the same 404 / 204+Retry-After / 200 / 204 contract —
 * see that class's Javadoc for the full state table.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
public class PivotablePlanHandler {

	protected final PivotableAsynchronousQueriesManager asyncManager;
	protected final IQueryPlanRegistry registry;

	/**
	 * @param serverRequest
	 *            carrying {@code queryUuid} as a path variable
	 * @return see {@code PivotablePlanController} for the contract
	 */
	public Mono<ServerResponse> getPlanSummary(ServerRequest serverRequest) {
		UUID queryUuid = UUID.fromString(serverRequest.pathVariable("queryUuid"));
		Optional<QueryPlan> plan = registry.findIdByUuid(queryUuid).flatMap(registry::snapshot);
		if (plan.isPresent()) {
			return ServerResponse.ok()
					.contentType(MediaType.APPLICATION_JSON)
					.body(BodyInserters.fromValue(QueryPlanSummary.of(plan.get(), Instant.now())));
		}
		return notReadyResponse(queryUuid);
	}

	/**
	 * @param serverRequest
	 *            carrying {@code queryUuid} as a path variable
	 * @return see {@code PivotablePlanController} for the contract
	 */
	public Mono<ServerResponse> getPlanSnapshot(ServerRequest serverRequest) {
		UUID queryUuid = UUID.fromString(serverRequest.pathVariable("queryUuid"));
		Optional<QueryPlan> plan = registry.findIdByUuid(queryUuid).flatMap(registry::snapshot);
		if (plan.isPresent()) {
			return ServerResponse.ok()
					.contentType(MediaType.APPLICATION_JSON)
					.body(BodyInserters.fromValue(plan.get()));
		}
		return notReadyResponse(queryUuid);
	}

	/**
	 * Build the not-200 response — 404 for never-seen UUIDs, 204 + {@code Retry-After: 1} while the engine is still
	 * planning, 204 for evicted/terminal-without-plan.
	 */
	protected Mono<ServerResponse> notReadyResponse(UUID queryUuid) {
		AsynchronousStatus status = asyncManager.getState(queryUuid);
		return switch (status) {
		case UNKNOWN -> ServerResponse.notFound().build();
		case RUNNING -> ServerResponse.noContent().header(HttpHeaders.RETRY_AFTER, "1").build();
		case SERVED, FAILED, DISCARDED -> ServerResponse.noContent().build();
		};
	}
}

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
import java.util.List;
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
import eu.solven.adhoc.query.AdhocQueryId;
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
	 * Composite-cube children — see {@code PivotablePlanController#getPlanChildren} for the full contract.
	 *
	 * @param serverRequest
	 *            carrying the parent's {@code queryUuid} as a path variable
	 * @return one summary per child plan, or the standard not-ready response
	 */
	public Mono<ServerResponse> getPlanChildren(ServerRequest serverRequest) {
		UUID queryUuid = UUID.fromString(serverRequest.pathVariable("queryUuid"));
		Optional<AdhocQueryId> parentId = registry.findIdByUuid(queryUuid);
		if (parentId.isEmpty()) {
			return notReadyResponse(queryUuid);
		}
		Instant now = Instant.now();
		List<QueryPlanSummary> children =
				registry.getChildrenOf(parentId.get()).stream().map(child -> QueryPlanSummary.of(child, now)).toList();
		return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(children));
	}

	/**
	 * Build the not-200 response. 404 is reserved for "the endpoint itself does not exist" (handled by Spring's default
	 * mapping); known endpoints always return 204. The SPA reads {@code Retry-After} to decide between "wait + retry"
	 * (queuing) and "stop polling" (terminal / unknown UUID).
	 */
	protected Mono<ServerResponse> notReadyResponse(UUID queryUuid) {
		AsynchronousStatus status = asyncManager.getState(queryUuid);
		return switch (status) {
		case RUNNING -> ServerResponse.noContent().header(HttpHeaders.RETRY_AFTER, "1").build();
		case UNKNOWN, SERVED, FAILED, DISCARDED -> ServerResponse.noContent().build();
		};
	}
}

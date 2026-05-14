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
import java.util.UUID;

import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.engine.observability.plan.IQueryPlanRegistry;
import eu.solven.adhoc.engine.observability.plan.QueryPlan;
import eu.solven.adhoc.engine.observability.plan.QueryPlanSummary;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

/**
 * WebFlux mirror of the webmvc {@code PivotablePlanController}. Reads from {@link IQueryPlanRegistry} and serves the
 * Live View's polling endpoints. The path variable is the {@link UUID} portion of an {@code AdhocQueryId}.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
public class PivotablePlanHandler {

	protected final IQueryPlanRegistry registry;

	/**
	 * @param serverRequest
	 *            carrying {@code queryUuid} as a path variable
	 * @return 200 with a {@link QueryPlanSummary} when a plan is registered; 204 No Content when no plan is registered
	 *         for that UUID (the registry is bounded — the plan may have been evicted, or the UUID may simply be
	 *         unknown). 404 is reserved for "the endpoint itself does not exist".
	 */
	public Mono<ServerResponse> getPlanSummary(ServerRequest serverRequest) {
		UUID queryUuid = UUID.fromString(serverRequest.pathVariable("queryUuid"));
		return registry.findIdByUuid(queryUuid)
				.flatMap(registry::snapshot)
				.map(plan -> QueryPlanSummary.of(plan, Instant.now()))
				.<Mono<ServerResponse>>map(summary -> ServerResponse.ok()
						.contentType(MediaType.APPLICATION_JSON)
						.body(BodyInserters.fromValue(summary)))
				.orElseGet(() -> ServerResponse.noContent().build());
	}

	/**
	 * @param serverRequest
	 *            carrying {@code queryUuid} as a path variable
	 * @return 200 with the full {@link QueryPlan} tree, or 204 No Content when no plan is registered for that UUID. 404
	 *         is reserved for "the endpoint itself does not exist".
	 */
	public Mono<ServerResponse> getPlanSnapshot(ServerRequest serverRequest) {
		UUID queryUuid = UUID.fromString(serverRequest.pathVariable("queryUuid"));
		return registry.findIdByUuid(queryUuid)
				.flatMap(registry::snapshot)
				.<Mono<ServerResponse>>map(plan -> ServerResponse.ok()
						.contentType(MediaType.APPLICATION_JSON)
						.body(BodyInserters.fromValue(plan)))
				.orElseGet(() -> ServerResponse.noContent().build());
	}
}

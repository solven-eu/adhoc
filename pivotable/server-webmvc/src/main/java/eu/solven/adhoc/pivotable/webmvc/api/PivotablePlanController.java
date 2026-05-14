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
package eu.solven.adhoc.pivotable.webmvc.api;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import eu.solven.adhoc.engine.observability.plan.IQueryPlanRegistry;
import eu.solven.adhoc.engine.observability.plan.QueryPlan;
import eu.solven.adhoc.engine.observability.plan.QueryPlanSummary;
import eu.solven.adhoc.pivotable.api.IPivotableApiConstants;
import eu.solven.adhoc.pivotable.query.AsynchronousStatus;
import eu.solven.adhoc.pivotable.query.PivotableAsynchronousQueriesManager;
import eu.solven.adhoc.pivotable.webnone.api.IPivotableRouteConstants;
import eu.solven.adhoc.query.AdhocQueryId;
import lombok.RequiredArgsConstructor;

/**
 * Read-only endpoints serving {@link QueryPlan} state for the UI Live View and CLI/programmatic monitors. The
 * {@code queryUuid} path variable is the Pivotable-side UUID the SPA already knows — same UUID the engine adopts as
 * {@link AdhocQueryId#getQueryId()} (the async-query manager binds it via {@code SubmittedQueryIdScope} before
 * submitting).
 *
 * <p>
 * Response contract — distinct states require distinct HTTP signals so the SPA's polling loop can react correctly:
 * <ul>
 * <li><strong>404 Not Found</strong> — the Pivotable manager has never seen this UUID (typo / stale link). The SPA
 * should drop the LiveView, not retry.</li>
 * <li><strong>204 No Content + {@code Retry-After: 1}</strong> — the manager accepted the submission but the engine
 * hasn't yet registered the plan (queueing or planning). The SPA shows a "Queuing…" state and keeps polling.</li>
 * <li><strong>200 OK</strong> — engine has registered a plan; body is a {@link QueryPlanSummary} or full
 * {@link QueryPlan}. The SPA renders normally.</li>
 * <li><strong>204 No Content</strong> (no {@code Retry-After}) — the manager knows the UUID but the registry has
 * evicted the plan (only happens after the query has terminated and the LRU pool flushed it). The SPA shows "may have
 * been evicted" and stops polling.</li>
 * </ul>
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@RestController
@RequestMapping(IPivotableApiConstants.PREFIX)
public class PivotablePlanController implements IPivotableRouteConstants {

	protected final PivotableAsynchronousQueriesManager asyncManager;
	protected final IQueryPlanRegistry registry;

	/**
	 * @param queryUuid
	 *            the Pivotable-side UUID returned by {@code POST /cubes/query/asynchronous}
	 * @return see class-level contract
	 */
	@GetMapping(value = R_CUBE_PLAN_SUMMARY, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<QueryPlanSummary> getPlanSummary(@PathVariable("queryUuid") UUID queryUuid) {
		Optional<QueryPlan> plan = lookupPlan(queryUuid);
		if (plan.isPresent()) {
			return ResponseEntity.ok(QueryPlanSummary.of(plan.get(), Instant.now()));
		}
		return notReadyResponse(queryUuid);
	}

	/**
	 * @param queryUuid
	 *            the Pivotable-side UUID returned by {@code POST /cubes/query/asynchronous}
	 * @return see class-level contract
	 */
	@GetMapping(value = R_CUBE_PLAN_SNAPSHOT, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<QueryPlan> getPlanSnapshot(@PathVariable("queryUuid") UUID queryUuid) {
		Optional<QueryPlan> plan = lookupPlan(queryUuid);
		if (plan.isPresent()) {
			return ResponseEntity.ok(plan.get());
		}
		return notReadyResponse(queryUuid);
	}

	protected Optional<QueryPlan> lookupPlan(UUID queryUuid) {
		return registry.findIdByUuid(queryUuid).flatMap(registry::snapshot);
	}

	/**
	 * Build the not-200 response — distinguishes unknown UUID (404), queuing (204 + Retry-After), and evicted (204).
	 * Used by both the summary and the snapshot endpoint, so the contract is uniform.
	 */
	protected <T> ResponseEntity<T> notReadyResponse(UUID queryUuid) {
		AsynchronousStatus status = asyncManager.getState(queryUuid);
		return switch (status) {
		case UNKNOWN -> ResponseEntity.notFound().build();
		case RUNNING -> ResponseEntity.noContent().header(HttpHeaders.RETRY_AFTER, "1").build();
		case SERVED, FAILED, DISCARDED -> ResponseEntity.noContent().build();
		};
	}
}

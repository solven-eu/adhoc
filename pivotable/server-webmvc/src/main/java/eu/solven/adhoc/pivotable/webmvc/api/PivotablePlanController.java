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
import java.util.List;
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
 * Response contract — 404 is reserved for "the endpoint itself does not exist" (Spring handles those misroutes
 * automatically). Every known endpoint always returns 200 or 204; the SPA reads {@code Retry-After} to decide between
 * "still working" and "give up":
 * <ul>
 * <li><strong>200 OK</strong> — engine has registered a plan; body is a {@link QueryPlanSummary} or full
 * {@link QueryPlan}. The SPA renders normally.</li>
 * <li><strong>204 No Content + {@code Retry-After: 1}</strong> — Pivotable accepted the submission but the engine
 * hasn't yet registered the plan (queueing or planning). The SPA shows a "Queuing…" state and keeps polling.</li>
 * <li><strong>204 No Content</strong> (no {@code Retry-After}) — no plan can be served for this UUID. May be because
 * the UUID is unknown to Pivotable (typo / stale link), because the plan was evicted post-termination, or because the
 * query has completed and its result has been served already. The SPA shows "no plan available" and stops polling.</li>
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

	/**
	 * Composite-cube children — when the supplied UUID is the parent of one or more sub-cube executions, returns one
	 * {@link QueryPlanSummary} per child. The contract mirrors the other plan endpoints:
	 * <ul>
	 * <li>200 with a (possibly empty) JSON array when the parent UUID is known to Pivotable AND its plan is in the
	 * registry. An empty array is the normal case for a non-composite query.</li>
	 * <li>204 + {@code Retry-After: 1} while Pivotable has the parent UUID but the engine has not yet registered the
	 * parent plan — sub-cubes can't have been spawned yet either.</li>
	 * <li>204 when the parent has been evicted post-termination.</li>
	 * <li>404 when the parent UUID was never seen by Pivotable.</li>
	 * </ul>
	 *
	 * @param queryUuid
	 *            the parent's Pivotable UUID
	 * @return one summary per child plan
	 */
	@GetMapping(value = R_CUBE_PLAN_CHILDREN, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<QueryPlanSummary>> getPlanChildren(@PathVariable("queryUuid") UUID queryUuid) {
		Optional<AdhocQueryId> parentId = registry.findIdByUuid(queryUuid);
		if (parentId.isEmpty()) {
			return notReadyResponse(queryUuid);
		}
		// Parent is registered → collect children. Returns an empty list for non-composite queries; that's still 200.
		Instant now = Instant.now();
		List<QueryPlanSummary> children =
				registry.getChildrenOf(parentId.get()).stream().map(child -> QueryPlanSummary.of(child, now)).toList();
		return ResponseEntity.ok(children);
	}

	protected Optional<QueryPlan> lookupPlan(UUID queryUuid) {
		return registry.findIdByUuid(queryUuid).flatMap(registry::snapshot);
	}

	/**
	 * Build the not-200 response. 404 is reserved for "the endpoint itself does not exist" (Spring handles that
	 * automatically when no controller matches the path); for known endpoints we always return 204 — the SPA reads the
	 * {@code Retry-After} header to decide between "wait + retry" (queuing) and "stop polling" (terminal / unknown
	 * UUID).
	 */
	protected <T> ResponseEntity<T> notReadyResponse(UUID queryUuid) {
		AsynchronousStatus status = asyncManager.getState(queryUuid);
		return switch (status) {
		case RUNNING -> ResponseEntity.noContent().header(HttpHeaders.RETRY_AFTER, "1").build();
		case UNKNOWN, SERVED, FAILED, DISCARDED -> ResponseEntity.noContent().build();
		};
	}
}

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
import java.util.UUID;

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
import eu.solven.adhoc.pivotable.webnone.api.IPivotableRouteConstants;
import eu.solven.adhoc.query.AdhocQueryId;
import lombok.RequiredArgsConstructor;

/**
 * Read-only endpoints serving {@link QueryPlan} state for the UI Live View and CLI/programmatic monitors.
 *
 * <p>
 * Two granularities:
 * <ul>
 * <li>{@code /plan/summary} — single small JSON object suitable for high-frequency polling (state, counts, elapsedMs,
 * startDelayMs, latestCompletedLabel). Cheap; backed by a single DAG walk that allocates only the summary value
 * object.</li>
 * <li>{@code /plan/snapshot} — full {@link QueryPlan} tree. Pay this once when the user opens the Live View modal; the
 * UI then keeps polling the cheap summary for the status chip.</li>
 * </ul>
 *
 * <p>
 * Path variable is the {@link UUID} portion of {@link AdhocQueryId#getQueryId()}, not the full
 * {@link AdhocQueryId#toString()} — URLs stay clean and copy/pasteable.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@RestController
@RequestMapping(IPivotableApiConstants.PREFIX)
public class PivotablePlanController implements IPivotableRouteConstants {

	protected final IQueryPlanRegistry registry;

	/**
	 * @param queryUuid
	 *            the UUID portion of an {@link AdhocQueryId}
	 * @return 200 with a {@link QueryPlanSummary} when a plan is registered; 204 No Content when no plan is registered
	 *         for that UUID (the registry is bounded — the plan may have been evicted, or the UUID may simply be
	 *         unknown). 404 is reserved for "the endpoint itself does not exist".
	 */
	@GetMapping(value = R_CUBE_PLAN_SUMMARY, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<QueryPlanSummary> getPlanSummary(@PathVariable("queryUuid") UUID queryUuid) {
		return registry.findIdByUuid(queryUuid)
				.flatMap(registry::snapshot)
				.map(plan -> QueryPlanSummary.of(plan, Instant.now()))
				.map(ResponseEntity::ok)
				.orElseGet(() -> ResponseEntity.noContent().build());
	}

	/**
	 * @param queryUuid
	 *            the UUID portion of an {@link AdhocQueryId}
	 * @return 200 with the full {@link QueryPlan} tree (deep-copied — safe to consume on the response side); 204 No
	 *         Content when no plan is registered for that UUID. 404 is reserved for "the endpoint itself does not
	 *         exist".
	 */
	@GetMapping(value = R_CUBE_PLAN_SNAPSHOT, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<QueryPlan> getPlanSnapshot(@PathVariable("queryUuid") UUID queryUuid) {
		return registry.findIdByUuid(queryUuid)
				.flatMap(registry::snapshot)
				.map(ResponseEntity::ok)
				.orElseGet(() -> ResponseEntity.noContent().build());
	}
}

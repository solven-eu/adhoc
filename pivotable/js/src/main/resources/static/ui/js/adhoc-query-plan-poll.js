// @ts-check

// Pure poll-state logic for the LiveView badge, extracted so it can be unit-tested without a DOM.
// The badge component imports `fetchSummary` and `nextPollState` from here and composes them with
// Vue refs + a setInterval timer.

// URL relative to authenticatedFetch's /api/v1 prefix — the fetch helper rejects URLs that start
// with /api, so we hand it the suffix only.
const SUMMARY_URL_PREFIX = "/cubes/queries/";
const SUMMARY_URL_SUFFIX = "/plan/summary";

/** @typedef {{state: string, totalNodes: number, doneNodes: number, pendingNodes: number, runningNodes: number, failedNodes: number, elapsedMs: number, startDelayMs: number, totalRowsOut: number, latestCompletedLabel: string | null}} PlanSummary */

/** @typedef {"polling" | "queuing" | "gone" | "unknown" | "unauthorized" | "error" | "terminal"} PollStatus */

/**
 * @typedef PollState
 * @property {PlanSummary | null} summary
 * @property {PollStatus} status
 *
 * @typedef {PlanSummary | "queuing" | "gone" | "unknown" | "unauthorized" | "error"} FetchResult
 *
 * @typedef PollOutcome
 * @property {PollState} state
 * @property {boolean} shouldStop
 *
 * @typedef {(url: string, fetchOptions?: any) => Promise<Response>} AuthenticatedFetch
 */

/**
 * @param {string} queryUuid
 * @param {AuthenticatedFetch} authenticatedFetch
 *   bound store method. Receives a URL relative to {@code /api/v1} and attaches the bearer token. On a 401 it
 *   flips {@code userStore.needsToLogin = true} (which opens the login modal) and either retries or — if the user
 *   is already logged out — throws a {@code UserNeedsToLoginError}.
 * @returns {Promise<FetchResult>}
 */
export async function fetchSummary(queryUuid, authenticatedFetch) {
	const url = `${SUMMARY_URL_PREFIX}${encodeURIComponent(queryUuid)}${SUMMARY_URL_SUFFIX}`;
	try {
		const response = await authenticatedFetch(url, { headers: { Accept: "application/json" } });
		// 401 here means authenticatedFetch already flipped needsToLogin (auto-retry failed or token expired
		// silently). We don't want the polling loop to busy-spin against an expired token — return the
		// `unauthorized` sentinel so the reducer stops.
		if (response.status === 401) return "unauthorized";
		if (response.status === 404) return "unknown";
		if (response.status === 204) {
			// Retry-After present = queuing (manager has the UUID, engine not yet ready). Absent = evicted.
			return response.headers.get("Retry-After") ? "queuing" : "gone";
		}
		if (!response.ok) return "error";
		return /** @type {PlanSummary} */ (await response.json());
	} catch (e) {
		// authenticatedFetch throws `UserNeedsToLoginError` when invoked while already logged out — same
		// outcome from the poller's perspective: stop and let the login modal take it from here.
		const name = e && /** @type {any} */ (e).name;
		if (name === "UserNeedsToLoginError") return "unauthorized";
		console.error("Issue polling plan summary:", e);
		return "error";
	}
}

/**
 * Pure reducer for one poll tick. Separated from the component so the state-machine can be unit-tested
 * without mocking timers, the DOM, or `fetch`. Transitions:
 * - `"unknown"` (404) → status `unknown`, shouldStop=true. UUID never seen; nothing to wait for.
 * - `"queuing"` (204 + Retry-After) → status `queuing`, shouldStop=false. Engine is planning; keep polling.
 * - `"gone"` (204 no Retry-After) → status `gone`, shouldStop=true. Plan was evicted post-termination.
 * - `"error"` (network glitch, 5xx) → status `error`, shouldStop=false. Keep last-known summary, keep polling
 *   — the next tick may recover.
 * - real `PlanSummary` with terminal state (DONE/FAILED) → status `terminal`, shouldStop=true.
 * - real `PlanSummary` mid-flight → status `polling`, shouldStop=false.
 *
 * @param {PollState} prev
 * @param {FetchResult} result
 * @returns {PollOutcome}
 */
export function nextPollState(prev, result) {
	if (result === "unknown") {
		return { state: { summary: prev.summary, status: "unknown" }, shouldStop: true };
	}
	if (result === "unauthorized") {
		// Stop polling; authenticatedFetch already flipped needsToLogin so the login modal opens.
		return { state: { summary: prev.summary, status: "unauthorized" }, shouldStop: true };
	}
	if (result === "queuing") {
		return { state: { summary: prev.summary, status: "queuing" }, shouldStop: false };
	}
	if (result === "gone") {
		return { state: { summary: prev.summary, status: "gone" }, shouldStop: true };
	}
	if (result === "error") {
		// Preserve the last successful summary so the UI keeps showing useful info during a transient blip.
		return { state: { summary: prev.summary, status: "error" }, shouldStop: false };
	}
	const terminal = result.state === "DONE" || result.state === "FAILED";
	return {
		state: { summary: result, status: terminal ? "terminal" : "polling" },
		shouldStop: terminal,
	};
}

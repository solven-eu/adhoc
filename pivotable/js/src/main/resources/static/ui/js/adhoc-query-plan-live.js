// @ts-check
import { onBeforeUnmount, ref, watch } from "vue";

// LiveView for an asynchronous query: polls /api/v1/cubes/queries/{queryUuid}/plan/summary at ~2 Hz. The server
// distinguishes four states via HTTP code + Retry-After header:
//   - 200            → plan registered; body is the summary.
//   - 204 + Retry-After → Pivotable has the UUID but the engine hasn't yet registered a plan (queueing/planning).
//                         Keep polling.
//   - 204            → terminal — plan was evicted (only after the query finished and LRU flushed it). Stop.
//   - 404            → the UUID was never seen by Pivotable (typo / stale link). Stop.
// The 5xx / network-error path is treated as a transient blip — keep polling and let the next tick recover.

const POLL_INTERVAL_MS = 500;

/** @typedef {{state: string, totalNodes: number, doneNodes: number, pendingNodes: number, runningNodes: number, failedNodes: number, elapsedMs: number, startDelayMs: number, totalRowsOut: number, latestCompletedLabel: string | null}} PlanSummary */

/** @typedef {"polling" | "queuing" | "gone" | "unknown" | "error" | "terminal"} PollStatus */

/**
 * @typedef PollState
 * @property {PlanSummary | null} summary
 * @property {PollStatus} status
 */

/**
 * @typedef {PlanSummary | "queuing" | "gone" | "unknown" | "error"} FetchResult
 *
 * @typedef PollOutcome
 * @property {PollState} state - new state to apply
 * @property {boolean} shouldStop - whether the caller should clear its polling timer
 */

/**
 * @param {string} queryUuid
 * @returns {Promise<FetchResult>}
 */
export async function fetchSummary(queryUuid) {
	const url = `/api/v1/cubes/queries/${encodeURIComponent(queryUuid)}/plan/summary`;
	try {
		const response = await fetch(url, { headers: { Accept: "application/json" } });
		if (response.status === 404) return "unknown";
		if (response.status === 204) {
			// Retry-After present = queuing (manager has the UUID, engine not yet ready). Absent = evicted.
			return response.headers.get("Retry-After") ? "queuing" : "gone";
		}
		if (!response.ok) return "error";
		return /** @type {PlanSummary} */ (await response.json());
	} catch (e) {
		console.error("Issue polling plan summary:", e);
		return "error";
	}
}

/**
 * Pure reducer for one poll tick. Separated from the component so the state-machine can be unit-tested without
 * mocking timers, the DOM, or `fetch`. The component just composes `fetchSummary` + `nextPollState` + a Vue
 * `ref` update.
 *
 * Transitions:
 * - `"unknown"` (404) → status `unknown`, shouldStop=true. UUID never seen; nothing to wait for.
 * - `"queuing"` (204 + Retry-After) → status `queuing`, shouldStop=false. Engine is planning; keep polling.
 * - `"gone"` (204 no Retry-After) → status `gone`, shouldStop=true. Plan was evicted post-termination.
 * - `"error"` (network glitch, 5xx) → status `error`, shouldStop=false. Keep last-known summary, keep polling —
 *   the next tick may recover.
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
	if (result === "queuing") {
		return { state: { summary: prev.summary, status: "queuing" }, shouldStop: false };
	}
	if (result === "gone") {
		return { state: { summary: prev.summary, status: "gone" }, shouldStop: true };
	}
	if (result === "error") {
		// Preserve the last successful summary so the UI keeps showing useful info during a transient blip,
		// instead of flashing back to a loading spinner.
		return { state: { summary: prev.summary, status: "error" }, shouldStop: false };
	}
	const terminal = result.state === "DONE" || result.state === "FAILED";
	return {
		state: { summary: result, status: terminal ? "terminal" : "polling" },
		shouldStop: terminal,
	};
}

export default {
	props: {
		queryUuid: {
			type: String,
			required: true,
		},
	},
	setup(props) {
		/** @type {import("vue").Ref<PlanSummary | null>} */
		const summary = ref(null);
		/** @type {import("vue").Ref<PollStatus>} */
		const status = ref("polling");

		/** @type {ReturnType<typeof setInterval> | null} */
		let timer = null;

		const stop = () => {
			if (timer !== null) {
				clearInterval(timer);
				timer = null;
			}
		};

		const tick = async () => {
			const result = await fetchSummary(props.queryUuid);
			const outcome = nextPollState({ summary: summary.value, status: status.value }, result);
			summary.value = outcome.state.summary;
			status.value = outcome.state.status;
			if (outcome.shouldStop) stop();
		};

		const start = () => {
			stop();
			tick();
			timer = setInterval(tick, POLL_INTERVAL_MS);
		};

		// Restart polling when the parent swaps the queryUuid prop — e.g. user navigates between queries without
		// unmounting the component.
		watch(
			() => props.queryUuid,
			(next) => {
				if (next) start();
				else stop();
			},
			{ immediate: true },
		);

		onBeforeUnmount(stop);

		return { summary, status };
	},
	template: /* HTML */ `
		<div class="adhoc-plan-live d-inline-flex align-items-center gap-2 small">
			<template v-if="status === 'unknown'">
				<i class="bi bi-exclamation-circle text-warning"></i>
				<span class="text-warning">Unknown query id.</span>
			</template>
			<template v-else-if="status === 'gone'">
				<i class="bi bi-question-circle text-muted"></i>
				<span class="text-muted">Plan no longer available (may have been evicted).</span>
			</template>
			<template v-else-if="status === 'queuing'">
				<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>
				<span class="text-muted">Queuing…</span>
			</template>
			<template v-else-if="summary === null">
				<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>
				<span>Loading plan…</span>
			</template>
			<template v-else>
				<span v-if="summary.state === 'PENDING'" class="badge bg-secondary">Queued</span>
				<span v-else-if="summary.state === 'RUNNING'" class="badge bg-primary">Running</span>
				<span v-else-if="summary.state === 'DONE'" class="badge bg-success">Done</span>
				<span v-else-if="summary.state === 'FAILED'" class="badge bg-danger">Failed</span>
				<span v-else class="badge bg-secondary">{{summary.state}}</span>

				<span v-if="status === 'polling'" class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>

				<span class="text-muted"> {{summary.doneNodes}}/{{summary.totalNodes}} step<span v-if="summary.totalNodes !== 1">s</span> </span>
				<span v-if="summary.failedNodes > 0" class="text-danger">({{summary.failedNodes}} failed)</span>

				<span class="text-muted" v-if="summary.startDelayMs > 0 && summary.state === 'PENDING'">
					waited {{Math.round(summary.startDelayMs / 100) / 10}}s
				</span>
				<span class="text-muted" v-else-if="summary.elapsedMs > 0"> {{Math.round(summary.elapsedMs / 100) / 10}}s </span>

				<span v-if="summary.latestCompletedLabel" class="text-muted text-truncate" style="max-width:18em">
					· last: <code>{{summary.latestCompletedLabel}}</code>
				</span>

				<span v-if="status === 'error'" class="text-warning"><i class="bi bi-exclamation-triangle"></i> network glitch</span>
			</template>
		</div>
	`,
};

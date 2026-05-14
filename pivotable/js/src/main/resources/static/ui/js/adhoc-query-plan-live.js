// @ts-check
import { onBeforeUnmount, ref, watch } from "vue";

// LiveView for a running query: polls /api/v1/cubes/queries/{queryUuid}/plan/summary at ~2 Hz and renders a small
// status line: state badge + "12/40 done" counts + elapsed/startDelay + last finished step. Stops polling on
// terminal state (DONE/FAILED) or 404, and on component unmount.
//
// Polling cadence is intentionally on the slow side (500ms) — the UI value is "is anything happening", not
// frame-perfect updates. Faster polls would burn cycles for tiny visual deltas.
//
// 204 path: a UUID with no registered plan is rendered as a discreet hint rather than an error, since the registry
// is bounded and an old query may have been evicted between the user's first click and the LiveView render. The
// server reserves 404 strictly for "the endpoint itself does not exist".

const POLL_INTERVAL_MS = 500;

/** @typedef {{state: string, totalNodes: number, doneNodes: number, pendingNodes: number, runningNodes: number, failedNodes: number, elapsedMs: number, startDelayMs: number, totalRowsOut: number, latestCompletedLabel: string | null}} PlanSummary */

/**
 * @param {string} queryUuid
 * @returns {Promise<PlanSummary | "not_found" | "error">}
 */
async function fetchSummary(queryUuid) {
	const url = `/api/v1/cubes/queries/${encodeURIComponent(queryUuid)}/plan/summary`;
	try {
		const response = await fetch(url, { headers: { Accept: "application/json" } });
		if (response.status === 204) return "not_found";
		if (!response.ok) return "error";
		return /** @type {PlanSummary} */ (await response.json());
	} catch (e) {
		console.error("Issue polling plan summary:", e);
		return "error";
	}
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
		/** @type {import("vue").Ref<"polling" | "not_found" | "error" | "terminal">} */
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
			if (result === "not_found") {
				status.value = "not_found";
				stop();
				return;
			}
			if (result === "error") {
				status.value = "error";
				return; // keep polling — transient network blip should recover on next tick
			}
			summary.value = result;
			if (result.state === "DONE" || result.state === "FAILED") {
				status.value = "terminal";
				stop();
			} else {
				status.value = "polling";
			}
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
			<template v-if="status === 'not_found'">
				<i class="bi bi-question-circle text-muted"></i>
				<span class="text-muted">No plan available (may have been evicted).</span>
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

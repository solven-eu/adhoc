// @ts-check
import { onBeforeUnmount, ref, watch } from "vue";

import AdhocQueryPlanMermaidModal from "./adhoc-query-plan-mermaid-modal.js";
import { fetchSummary, nextPollState } from "./adhoc-query-plan-poll.js";
import { useUserStore } from "./store-user.js";

// LiveView for an asynchronous query: polls /api/v1/cubes/queries/{queryUuid}/plan/summary at ~2 Hz. The pure
// polling logic (`fetchSummary` + `nextPollState`) lives in `adhoc-query-plan-poll.js` so it can be Vitest-tested
// without a DOM; this file wires it to a Vue component + Bootstrap modal trigger.

const POLL_INTERVAL_MS = 500;

/** @typedef {import("./adhoc-query-plan-poll.js").PlanSummary} PlanSummary */
/** @typedef {import("./adhoc-query-plan-poll.js").PollStatus} PollStatus */

export default {
	components: { AdhocQueryPlanMermaidModal },
	props: {
		queryUuid: {
			type: String,
			required: true,
		},
	},
	setup(props) {
		const userStore = useUserStore();
		// Bind once so the closure sees the same authenticated-fetch implementation every tick. The helper
		// attaches the bearer token, handles 401 (flips needsToLogin → opens the login modal) and rejects URLs
		// that already start with /api.
		const authFetch = (url, options) => userStore.authenticatedFetch(url, options);

		/** @type {import("vue").Ref<PlanSummary | null>} */
		const summary = ref(null);
		/** @type {import("vue").Ref<PollStatus>} */
		const status = ref("polling");
		// Controls the Mermaid detail modal. Toggled by the "Details" button; the modal emits an update
		// when the user closes it (ESC, backdrop, ✕) so we know to allow a fresh open later.
		const showPlanModal = ref(false);
		const openPlanModal = () => {
			showPlanModal.value = true;
		};

		/** @type {ReturnType<typeof setInterval> | null} */
		let timer = null;

		const stop = () => {
			if (timer !== null) {
				clearInterval(timer);
				timer = null;
			}
		};

		const tick = async () => {
			const result = await fetchSummary(props.queryUuid, authFetch);
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

		return { summary, status, showPlanModal, openPlanModal };
	},
	template: /* HTML */ `
		<div class="adhoc-plan-live d-inline-flex align-items-center gap-2 small">
			<template v-if="status === 'unknown'">
				<i class="bi bi-exclamation-circle text-warning"></i>
				<span class="text-warning">Unknown query id.</span>
			</template>
			<template v-else-if="status === 'unauthorized'">
				<i class="bi bi-lock text-muted"></i>
				<span class="text-muted">Log in to see the plan.</span>
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

				<button
					type="button"
					class="btn btn-sm btn-link p-0 ms-1 text-decoration-none"
					title="Show the query plan as a Mermaid graph"
					@click="openPlanModal"
				>
					<i class="bi bi-diagram-3"></i> Details
				</button>
			</template>
		</div>
		<AdhocQueryPlanMermaidModal :queryUuid="queryUuid" v-model:show="showPlanModal" />
	`,
};

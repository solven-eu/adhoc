// @ts-check
import { computed, ref, watch, onBeforeUnmount } from "vue";

import { formatTimings, hasActiveTiming } from "./adhoc-query-grid-timings.js";
import AdhocQueryGridTimingsModal from "./adhoc-query-grid-timings-modal.js";

// Small presentational component rendering a compact performance summary under the grid.
//
// Display policy:
//   - While a stage is active: show ONLY that stage's live duration. Keeps the bar quiet during
//     query execution and focuses the user's eye on what is currently happening.
//   - When idle (all stages finished): show only the total. The per-stage breakdown is one click
//     away in the modal.
// Click the bar to open the full per-stage timing breakdown in a modal. The previous design
// listed every stage inline, which became noisy at runtime (six entries with constantly ticking
// digits) and was hard to read on narrow viewports.
export default {
	components: { AdhocQueryGridTimingsModal },
	props: {
		tabularView: {
			type: Object,
			required: true,
		},
	},
	setup(props) {
		const now = ref(Date.now());
		// Fires when anyActive transitions from false→true or true→false. Start/stop the tick
		// accordingly. The 100 ms cadence is fine-grained enough for the digits to feel live
		// but cheap in compute.
		/** @type {ReturnType<typeof setInterval> | null} */
		let intervalId = null;
		const anyActive = computed(() => hasActiveTiming(props.tabularView && props.tabularView.timing));
		watch(
			anyActive,
			(active) => {
				if (active && !intervalId) {
					intervalId = setInterval(() => {
						now.value = Date.now();
					}, 100);
				} else if (!active && intervalId) {
					clearInterval(intervalId);
					intervalId = null;
				}
			},
			{ immediate: true },
		);
		onBeforeUnmount(() => {
			if (intervalId) clearInterval(intervalId);
			intervalId = null;
		});

		const timings = computed(() => formatTimings(props.tabularView && props.tabularView.timing, now.value));

		// The single stage currently in-flight (or null when idle). Bar shows just this entry
		// while non-null; falls back to the total otherwise.
		const activeEntry = computed(() => {
			if (!timings.value) return null;
			return timings.value.entries.find((e) => e.active) || null;
		});

		const showModal = ref(false);
		const openModal = () => {
			showModal.value = true;
		};

		return { timings, activeEntry, showModal, openModal };
	},
	template: /* HTML */ `
		<!--
			The whole line is a button so the entire chip is the click target — easier to hit
			than a tiny icon and keeps the interaction discoverable. The btn-link class strips
			the button chrome so it still looks like a passive metrics line.
		-->
		<button
			v-if="timings"
			type="button"
			class="btn btn-link btn-sm p-0 small text-muted mt-1 text-decoration-none"
			title="Click to see the per-stage breakdown"
			@click="openModal"
		>
			<i class="bi bi-speedometer2 me-1"></i>
			<span class="fw-semibold">Performance:</span>
			<span v-if="activeEntry" class="text-primary ms-1 fw-semibold">{{activeEntry.stage}}={{activeEntry.ms}}ms…</span>
			<span v-else class="ms-1">total: {{timings.total}}ms</span>
		</button>
		<AdhocQueryGridTimingsModal :tabularView="tabularView" v-model:show="showModal" />
	`,
};

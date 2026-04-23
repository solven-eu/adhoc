import { computed } from "vue";

import { formatTimings } from "./adhoc-query-grid-timings.js";

// Small presentational component rendering the per-stage timings strip under the grid. Accepts
// the full `tabularView` and derives its own { entries, total } view via `formatTimings()`.
// Renders nothing when no timings are available (first render, cached views). Muted styling +
// explicit "Performance" label make it obvious these are operational, non-functional metrics.
export default {
	props: {
		tabularView: {
			type: Object,
			required: true,
		},
	},
	setup(props) {
		const timings = computed(() => formatTimings(props.tabularView && props.tabularView.timing));
		return { timings };
	},
	template: /* HTML */ `
		<div v-if="timings" class="small text-muted mt-1" title="Operational metrics — not part of the query result">
			<i class="bi bi-speedometer2 me-1"></i>
			<span class="fw-semibold me-1">Performance:</span>
			<span v-for="(entry, i) in timings.entries" :key="entry.stage" class="me-2">
				{{entry.stage}}={{entry.ms}}ms<span v-if="i < timings.entries.length - 1">,</span>
			</span>
			<span class="ms-1">(total: {{timings.total}}ms)</span>
		</div>
	`,
};

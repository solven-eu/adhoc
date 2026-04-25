import { computed, ref, watch, onBeforeUnmount } from "vue";

import { formatTimings, hasActiveTiming } from "./adhoc-query-grid-timings.js";

// Small presentational component rendering the per-stage timings strip under the grid. Accepts
// the full `tabularView` and derives its own { entries, total, anyActive } view via
// `formatTimings()`. Renders nothing when no timings are available (first render, cached views).
// Muted styling + explicit "Performance" label make it obvious these are operational,
// non-functional metrics.
//
// While any stage is in-flight (see `_startedAt` sibling keys in `timing`), the component ticks
// a `now` reference on a 100 ms interval so the active stage shows a growing duration rather
// than sitting at its last-rendered value. The interval is torn down as soon as every stage has
// a final duration, so an idle grid pays nothing.
export default {
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
		return { timings };
	},
	template: /* HTML */ `
		<div v-if="timings" class="small text-muted mt-1" title="Operational metrics — not part of the query result">
			<i class="bi bi-speedometer2 me-1"></i>
			<span class="fw-semibold me-1">Performance:</span>
			<span v-for="(entry, i) in timings.entries" :key="entry.stage" class="me-2">
				<span v-if="entry.active" class="text-primary fw-semibold">{{entry.stage}}={{entry.ms}}ms…</span>
				<span v-else>{{entry.stage}}={{entry.ms}}ms</span>
				<span v-if="i < timings.entries.length - 1">,</span>
			</span>
			<span class="ms-1" :class="{'text-primary': timings.anyActive}"
				>({{timings.anyActive ? 'elapsed' : 'total'}}: {{timings.total}}ms<span v-if="timings.anyActive">…</span>)</span
			>
		</div>
	`,
};

// @ts-check
import { computed, ref, watch, onBeforeUnmount, onMounted } from "vue";
import { Modal } from "bootstrap";

import { formatTimings, hasActiveTiming } from "./adhoc-query-grid-timings.js";

// Modal listing every per-stage timing as a table. Companion to adhoc-query-grid-timings-bar.js,
// which renders a compact one-line summary and triggers this modal on click.
//
// Re-uses formatTimings + hasActiveTiming so the modal sees the same data as the bar; ticks a
// 100ms `now` ref while ANY stage is active so the in-flight row keeps growing while the user
// has the modal open.

export default {
	props: {
		tabularView: {
			type: Object,
			required: true,
		},
		show: {
			type: Boolean,
			default: false,
		},
	},
	emits: ["update:show"],
	setup(props, { emit }) {
		const now = ref(Date.now());

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

		/** @type {Modal | null} */
		let bootstrapModal = null;
		const modalRef = ref(null);

		onMounted(() => {
			if (!modalRef.value) return;
			bootstrapModal = new Modal(/** @type {HTMLElement} */ (modalRef.value), {});
			// Bootstrap's hidden event fires on ESC / backdrop / ✕ — propagate back so the parent
			// can re-open later via the same `show` prop. Without this, `show` stays stuck at true
			// after the user closes once.
			/** @type {HTMLElement} */ (modalRef.value).addEventListener("hidden.bs.modal", () => {
				emit("update:show", false);
			});
		});

		watch(
			() => props.show,
			(next) => {
				if (!bootstrapModal) return;
				if (next) bootstrapModal.show();
				else bootstrapModal.hide();
			},
		);

		return { timings, modalRef };
	},
	template: /* HTML */ `
		<div class="modal fade" tabindex="-1" aria-labelledby="timingsModalLabel" aria-hidden="true" :ref="(el) => (modalRef = el)">
			<div class="modal-dialog modal-dialog-centered">
				<div class="modal-content">
					<div class="modal-header">
						<h5 class="modal-title" id="timingsModalLabel">
							<i class="bi bi-speedometer2 me-1"></i>
							Query timing breakdown
						</h5>
						<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
					</div>
					<div class="modal-body">
						<div v-if="!timings" class="text-muted small">No timings recorded yet.</div>
						<table v-else class="table table-sm mb-0">
							<thead>
								<tr>
									<th class="text-muted small fw-normal">Stage</th>
									<th class="text-muted small fw-normal text-end">Duration</th>
								</tr>
							</thead>
							<tbody>
								<tr v-for="entry in timings.entries" :key="entry.stage" :class="{'fw-semibold text-primary': entry.active}">
									<td>{{entry.stage}}<span v-if="entry.active" class="ms-1">…</span></td>
									<td class="text-end font-monospace">{{entry.ms}} ms</td>
								</tr>
								<tr class="border-top">
									<td class="text-muted">{{timings.anyActive ? 'Elapsed so far' : 'Total'}}</td>
									<td class="text-end font-monospace fw-semibold">{{timings.total}} ms<span v-if="timings.anyActive">…</span></td>
								</tr>
							</tbody>
						</table>
					</div>
				</div>
			</div>
		</div>
	`,
};

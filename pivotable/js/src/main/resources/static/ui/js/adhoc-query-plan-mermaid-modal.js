// @ts-check
import { onMounted, ref, watch } from "vue";
import { Modal } from "bootstrap";
import mermaid from "mermaid";

import { planToMermaid } from "./adhoc-query-plan-mermaid.js";
import { useUserStore } from "./store-user.js";

// Modal that renders the current plan as a Mermaid `graph TD` diagram. Fetches /plan/snapshot the
// first time the user opens it (and every time the queryUuid changes); renders via mermaid.render.
//
// Lifecycle:
//   - mounted: instantiate the Bootstrap Modal wrapper.
//   - the parent (the LiveView chip) flips `show` to true → we open the modal, fetch the snapshot,
//     convert + render.
//   - the Bootstrap close button / ESC / backdrop click → user-driven close → reset `show` so a
//     subsequent open re-fetches a fresh snapshot (the plan may have evolved between opens).
//
// We intentionally don't poll while open — the user opened the modal to inspect a snapshot; updating
// the graph under their finger would be jarring. A "Refresh" button forces a re-fetch.

let nextSvgId = 0;
const genSvgId = () => `plan-mermaid-${nextSvgId++}`;

mermaid.initialize({
	startOnLoad: false,
	securityLevel: "loose",
	logLevel: 5,
});

export default {
	props: {
		queryUuid: {
			type: String,
			required: true,
		},
		show: {
			type: Boolean,
			default: false,
		},
	},
	emits: ["update:show"],
	setup(props, { emit }) {
		const userStore = useUserStore();

		const mermaidSvg = ref("");
		const isLoading = ref(false);
		const errorText = ref("");

		/** @type {Modal | null} */
		let bootstrapModal = null;
		/** @type {import("vue").Ref<HTMLElement | null>} */
		const modalRef = ref(null);

		const renderFromUuid = async (uuid) => {
			if (!uuid) return;
			isLoading.value = true;
			errorText.value = "";
			mermaidSvg.value = "";
			try {
				// Use authenticatedFetch so the bearer is attached and a 401 flips
				// userStore.needsToLogin (which opens the login modal). Plain fetch on `/api/**` is
				// rejected: only the `/api/login/v1/**` chain accepts cookies.
				const response = await userStore.authenticatedFetch(`/cubes/queries/${encodeURIComponent(uuid)}/plan/snapshot`, {
					headers: { Accept: "application/json" },
				});
				if (response.status === 401) {
					errorText.value = "Please log in to view the plan.";
					return;
				}
				if (response.status === 404) {
					errorText.value = "Unknown query id.";
					return;
				}
				if (response.status === 204) {
					errorText.value = response.headers.get("Retry-After")
						? "Plan is still being prepared. Try again in a moment."
						: "Plan no longer available (may have been evicted).";
					return;
				}
				if (!response.ok) {
					errorText.value = `Failed to fetch plan snapshot: HTTP ${response.status}`;
					return;
				}
				const plan = await response.json();
				const source = planToMermaid(plan);
				const id = genSvgId();
				const res = await mermaid.render(id, source);
				mermaidSvg.value = res.svg;
			} catch (e) {
				// authenticatedFetch throws UserNeedsToLoginError when invoked while already logged
				// out — it has already flipped needsToLogin to open the login modal. We just need to
				// keep the snapshot modal quiet so the two modals don't fight over focus.
				const name = e && /** @type {any} */ (e).name;
				if (name === "UserNeedsToLoginError") {
					errorText.value = "Please log in to view the plan.";
					return;
				}
				console.error("Issue rendering plan as Mermaid:", e);
				errorText.value = String(e && /** @type {Error} */ (e).message ? /** @type {Error} */ (e).message : e);
			} finally {
				isLoading.value = false;
			}
		};

		onMounted(() => {
			if (!modalRef.value) return;
			bootstrapModal = new Modal(/** @type {HTMLElement} */ (modalRef.value), {});
			// Bootstrap fires `hidden.bs.modal` when the user closes via ✕, ESC, or backdrop click. Sync
			// our reactive `show` flag back to false so the parent can detect close and re-open later.
			/** @type {HTMLElement} */ (modalRef.value).addEventListener("hidden.bs.modal", () => {
				emit("update:show", false);
			});
		});

		watch(
			() => props.show,
			(next) => {
				if (!bootstrapModal) return;
				if (next) {
					bootstrapModal.show();
					renderFromUuid(props.queryUuid);
				} else {
					bootstrapModal.hide();
				}
			},
		);

		// If the parent swaps queryUuid while the modal is open (rare but legitimate — user navigates
		// between queries with the modal pinned), re-fetch.
		watch(
			() => props.queryUuid,
			(next) => {
				if (props.show && next) renderFromUuid(next);
			},
		);

		const refresh = () => renderFromUuid(props.queryUuid);

		return { mermaidSvg, isLoading, errorText, modalRef, refresh };
	},
	template: /* HTML */ `
		<div class="modal fade" tabindex="-1" aria-labelledby="planMermaidModalLabel" aria-hidden="true" :ref="(el) => (modalRef = el)">
			<div class="modal-dialog modal-dialog-centered modal-xl modal-fullscreen-lg-down">
				<div class="modal-content">
					<div class="modal-header">
						<h5 class="modal-title" id="planMermaidModalLabel">Query plan</h5>
						<div class="ms-auto d-flex gap-2 align-items-center">
							<button type="button" class="btn btn-sm btn-outline-secondary" @click="refresh" :disabled="isLoading">
								<i class="bi bi-arrow-clockwise"></i> Refresh
							</button>
							<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
						</div>
					</div>
					<div class="modal-body">
						<div v-if="isLoading && !mermaidSvg" class="d-flex align-items-center gap-2 text-muted py-4">
							<span class="spinner-border" role="status" aria-hidden="true"></span>
							<span>Loading plan…</span>
						</div>
						<div v-if="errorText" class="alert alert-warning small">{{errorText}}</div>
						<pre class="mermaid" v-html="mermaidSvg" />
					</div>
				</div>
			</div>
		</div>
	`,
};

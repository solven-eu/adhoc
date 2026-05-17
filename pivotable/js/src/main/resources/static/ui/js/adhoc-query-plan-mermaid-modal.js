// @ts-check
import { onMounted, ref, watch } from "vue";
import { Modal } from "bootstrap";
import mermaid from "mermaid";

import { planToMermaid, collectSqlLeaves, STATE_PALETTE } from "./adhoc-query-plan-mermaid.js";
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
		/** @type {import("vue").Ref<{label: string, sql: string}[]>} */
		const sqlLeaves = ref([]);
		// Flowchart orientation. "TD" (top-down) was the original layout; "LR" (left-to-right) is
		// often more readable for wide plans (many SQL leaves under a single CubeQueryStep) because
		// Mermaid stops compressing node widths to fit a fixed canvas. Modal-local — we don't persist
		// it across opens because the right direction is plan-shape-dependent and the user typically
		// wants a clean default each time.
		/** @type {import("vue").Ref<"TD"|"LR">} */
		const direction = ref("TD");
		// The most recently fetched plan snapshot, kept so flipping `direction` can re-render without
		// a network round-trip. Reset on each fetch.
		/** @type {import("vue").Ref<any>} */
		const lastPlan = ref(null);
		/**
		 * Maps an SQL string to the "Copied!" feedback flag for the corresponding row's button. Indexed by
		 * the SQL itself (which is unique per row — `collectSqlLeaves` dedupes) so the flag survives a list
		 * resort or filter without leaking to a different row.
		 *
		 * @type {import("vue").Ref<Record<string, boolean>>}
		 */
		const copiedFlags = ref({});

		/** @type {Modal | null} */
		let bootstrapModal = null;
		/** @type {import("vue").Ref<HTMLElement | null>} */
		const modalRef = ref(null);

		/**
		 * Render a plan snapshot to SVG using the current `direction`. Extracted so the direction
		 * toggle can re-render without a network round-trip.
		 *
		 * @param {any} plan
		 */
		const renderPlan = async (plan) => {
			if (!plan) return;
			const source = planToMermaid(plan, direction.value);
			const id = genSvgId();
			const res = await mermaid.render(id, source);
			mermaidSvg.value = res.svg;
		};

		const renderFromUuid = async (uuid) => {
			if (!uuid) return;
			isLoading.value = true;
			errorText.value = "";
			mermaidSvg.value = "";
			sqlLeaves.value = [];
			copiedFlags.value = {};
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
				lastPlan.value = plan;
				await renderPlan(plan);
				// Collected AFTER the graph renders so a failure to render mermaid doesn't suppress the list —
				// the user still sees the SQL queries that ran even if the diagram itself blew up.
				sqlLeaves.value = collectSqlLeaves(plan);
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

		const toggleDirection = () => {
			direction.value = direction.value === "TD" ? "LR" : "TD";
			// Re-render the cached snapshot instead of refetching — the plan didn't change, only its
			// rendering. If the user has never opened the modal (no plan cached), we silently no-op
			// because the button is only shown once an SVG has been rendered.
			if (lastPlan.value) {
				renderPlan(lastPlan.value).catch((e) => console.error("Issue re-rendering plan after direction toggle:", e));
			}
		};

		/**
		 * Copy the SQL string to the clipboard and surface a transient "Copied!" feedback on the matching
		 * button. Uses `navigator.clipboard.writeText` (the only API still in spec); falls back silently when
		 * the page is not served from a secure context (`navigator.clipboard` is undefined under plain http).
		 */
		const copySql = async (sql) => {
			try {
				if (!navigator.clipboard || !navigator.clipboard.writeText) {
					console.warn("navigator.clipboard not available (insecure context?) — SQL not copied");
					return;
				}
				await navigator.clipboard.writeText(sql);
				copiedFlags.value = { ...copiedFlags.value, [sql]: true };
				// 1.5s is short enough that the user doesn't think the row is permanently "stuck" copied, long
				// enough to notice the green tick. Reset by spreading the current map to drop just this key.
				setTimeout(() => {
					const next = { ...copiedFlags.value };
					delete next[sql];
					copiedFlags.value = next;
				}, 1500);
			} catch (e) {
				console.error("Failed copying SQL to clipboard:", e);
			}
		};

		// `STATE_PALETTE` is the same source of truth that `planToMermaid` uses to emit its
		// `classDef` lines, so the legend swatches can't drift from the colours Mermaid paints.
		// Convert the record into an array so the template's `v-for` doesn't depend on the key
		// iteration order of the underlying object (more explicit, easier to reason about).
		const stateLegend = /** @type {const} */ (["done", "running", "pending", "failed"]).map((key) => ({
			key,
			fill: STATE_PALETTE[key].fill,
			stroke: STATE_PALETTE[key].stroke,
			label: STATE_PALETTE[key].label,
		}));

		return { mermaidSvg, isLoading, errorText, modalRef, refresh, sqlLeaves, copiedFlags, copySql, direction, toggleDirection, stateLegend };
	},
	template: /* HTML */ `
		<div class="modal fade" tabindex="-1" aria-labelledby="planMermaidModalLabel" aria-hidden="true" :ref="(el) => (modalRef = el)">
			<div class="modal-dialog modal-dialog-centered modal-xl modal-fullscreen-lg-down">
				<div class="modal-content">
					<div class="modal-header">
						<h5 class="modal-title" id="planMermaidModalLabel">Query plan</h5>
						<div class="ms-auto d-flex gap-2 align-items-center">
							<!--
								Flowchart-direction toggle. The icon flips between a "down arrow" (currently TD,
								click to switch to LR) and a "right arrow" (currently LR, click to switch to TD)
								so the next state is immediately readable from the glyph. Disabled while loading
								because the cached lastPlan may not yet be populated.
							-->
							<button
								type="button"
								class="btn btn-sm btn-outline-secondary"
								@click="toggleDirection"
								:disabled="isLoading"
								:title="direction === 'TD' ? 'Switch to left-to-right layout' : 'Switch to top-down layout'"
							>
								<i :class="direction === 'TD' ? 'bi bi-arrow-down' : 'bi bi-arrow-right'"></i>
								{{ direction === "TD" ? "Top-down" : "Left-to-right" }}
							</button>
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
						<!--
							Legend — explains the per-state node colours (same palette used by Mermaid's
							classDef lines, sourced from STATE_PALETTE so swatches and chart cannot drift).
							Only shown once the SVG is rendered: an empty diagram has nothing to legend.
						-->
						<div v-if="mermaidSvg" class="d-flex flex-wrap gap-3 align-items-center small text-muted mt-2">
							<span>Legend:</span>
							<span v-for="entry in stateLegend" :key="entry.key" class="d-inline-flex align-items-center gap-1">
								<span
									aria-hidden="true"
									:style="'display:inline-block;width:14px;height:14px;border-radius:3px;background:' + entry.fill + ';border:1px solid ' + entry.stroke + ';'"
								></span>
								{{ entry.label }}
							</span>
						</div>
						<div v-if="sqlLeaves.length > 0" class="mt-3">
							<h6 class="text-muted">SQL queries in this plan</h6>
							<div v-for="(leaf, i) in sqlLeaves" :key="i" class="border rounded p-2 mb-2">
								<div class="d-flex justify-content-between align-items-start gap-2">
									<pre class="mb-0 small flex-grow-1" style="white-space: pre-wrap; word-break: break-all;">{{leaf.sql}}</pre>
									<button
										type="button"
										class="btn btn-sm btn-outline-secondary flex-shrink-0"
										@click="copySql(leaf.sql)"
										:title="copiedFlags[leaf.sql] ? 'Copied!' : 'Copy to clipboard'"
									>
										<i v-if="copiedFlags[leaf.sql]" class="bi bi-check2 text-success"></i>
										<i v-else class="bi bi-clipboard"></i>
										<span class="ms-1">{{copiedFlags[leaf.sql] ? "Copied" : "Copy"}}</span>
									</button>
								</div>
							</div>
						</div>
					</div>
				</div>
			</div>
		</div>
	`,
};

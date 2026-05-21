// @ts-check
//
// Modal that surfaces the personal query-history graph as a Mermaid `graph TD` diagram +
// an interactive list of past queries. Phase 3 of the history roadmap.
//
// Lifecycle mirrors `adhoc-query-plan-mermaid-modal.js`:
//   - Parent flips `show` to true → we open the bootstrap modal, snapshot the persistent
//     store, render the graph + list.
//   - Bootstrap close / ESC / backdrop click → user-driven close → emit `update:show` so
//     the parent can open us again later (re-snapshot on each open keeps the view fresh
//     after eviction or a fresh capture from the parent's watch).
//   - Click a node in the graph (or "Restore" on a list row) → emit `restore` with the
//     target snapshot. The parent calls `parsedJsonToQueryModel` to hydrate queryModel.
//   - "Forget" on a list row → store.forget(hash) + re-render.
//   - "Clear all" → store.clearAll() + re-render.

import { onMounted, onBeforeUnmount, reactive, ref, watch } from "vue";
import { Modal } from "bootstrap";
import mermaid from "mermaid";

import { collectDistinctNames, contentHash, filterSnapshotByNames, useQueryHistoryStore } from "./adhoc-query-history-store.js";
import { snapshotToMermaid } from "./adhoc-query-history-mermaid.js";

let nextSvgId = 0;
const genSvgId = () => `history-mermaid-${nextSvgId++}`;

// Window-scoped click callback name. Each modal mount registers/clears its own slot keyed
// by a generation counter so concurrent modal instances (unlikely but technically possible
// after rapid open/close) don't trample each other.
let nextClickGen = 0;

mermaid.initialize({
	startOnLoad: false,
	securityLevel: "loose",
	logLevel: 5,
});

export default {
	props: {
		show: {
			type: Boolean,
			default: false,
		},
		cubeId: {
			type: String,
			required: true,
		},
		// The current query snapshot — used to highlight the matching node in the graph.
		// Passed as a parsedJson, not a queryModel ref, so this modal is a pure consumer.
		currentSnapshot: {
			type: Object,
			default: null,
		},
		// Bumped by the parent on every successful query capture. We watch it so an open modal
		// re-renders when a new query lands instead of showing a stale graph.
		bumpVersion: {
			type: Number,
			default: 0,
		},
	},
	emits: ["update:show", "restore"],
	setup(props, { emit }) {
		/** @type {import("vue").Ref<string>} */
		const mermaidSvg = ref("");
		/** @type {import("vue").Ref<string>} */
		const errorText = ref("");
		/** @type {import("vue").Ref<"TD" | "LR">} */
		const direction = ref("TD");
		/** @type {import("vue").Ref<any>} */
		const lastSnapshot = ref(null);
		/** @type {import("vue").Ref<any[]>} */
		const nodeList = ref([]);
		const currentHash = ref(/** @type {string | null} */ (null));
		// Universe of distinct names across the loaded snapshot — populated by renderNow,
		// drives the chip strip in the template. Sorted alpha so chip order stays stable.
		/** @type {import("vue").Ref<{ columns: string[], measures: string[] }>} */
		const distinctNames = ref({ columns: [], measures: [] });

		// Tri-state chip filter. Map keys are the names; values are "include" or "exclude". A
		// name absent from the map is in the "default" (no-filter) state. Using `reactive` here
		// (not `ref(new Map())`) so per-key writes via `filterStates.set(...)` trigger updates
		// without us having to replace the whole map on every click.
		/** @type {{ columns: Map<string, "include" | "exclude">, measures: Map<string, "include" | "exclude"> }} */
		const filterStates = reactive({
			columns: new Map(),
			measures: new Map(),
		});

		const history = useQueryHistoryStore(props.cubeId);

		/** @type {Modal | null} */
		let bootstrapModal = null;
		/** @type {import("vue").Ref<HTMLElement | null>} */
		const modalRef = ref(null);

		// Window-scoped click slot for this modal instance. Mermaid's `click N call onHistoryNodeClick("hash")`
		// invokes `window.onHistoryNodeClick(hash)`; we bind/unbind a fresh slot per mount so multiple modals
		// (or a remount after an in-place re-login) can coexist without trampling each other.
		const clickFnName = `__pivotableHistoryNodeClick_${nextClickGen++}`;

		/**
		 * Partition the current `filterStates` maps into the includes/excludes lists that
		 * `filterSnapshotByNames` expects.
		 */
		const partitionFilter = () => {
			const includeColumns = /** @type {string[]} */ ([]);
			const excludeColumns = /** @type {string[]} */ ([]);
			const includeMeasures = /** @type {string[]} */ ([]);
			const excludeMeasures = /** @type {string[]} */ ([]);
			for (const [name, state] of filterStates.columns) {
				if (state === "include") includeColumns.push(name);
				else if (state === "exclude") excludeColumns.push(name);
			}
			for (const [name, state] of filterStates.measures) {
				if (state === "include") includeMeasures.push(name);
				else if (state === "exclude") excludeMeasures.push(name);
			}
			return { includeColumns, excludeColumns, includeMeasures, excludeMeasures };
		};

		/**
		 * Render the current snapshot to SVG and rebuild the node list, after applying the
		 * tri-state chip filter on top.
		 */
		const renderNow = async () => {
			errorText.value = "";
			try {
				// Always reload the raw snapshot so a fresh capture upstream is reflected. We
				// recompute `distinctNames` from the RAW snapshot (so chips never disappear
				// just because the user excluded something — they need the chip to toggle it
				// back). Filtering happens on a separate variable.
				const rawSnap = history.snapshot();
				lastSnapshot.value = rawSnap;
				distinctNames.value = collectDistinctNames(rawSnap);
				const ch = props.currentSnapshot ? contentHash(props.currentSnapshot) : null;
				currentHash.value = ch;

				const filteredSnap = filterSnapshotByNames(rawSnap, partitionFilter());

				// Build the node list ordered by lastSeenAt desc — most relevant on top.
				// The list shows the filtered set so it stays in lock-step with the graph.
				nodeList.value = Object.values(filteredSnap.nodes || {})
					.slice()
					.sort((a, b) => {
						const ta = Date.parse(a.lastSeenAt || "") || 0;
						const tb = Date.parse(b.lastSeenAt || "") || 0;
						return tb - ta;
					});

				const source = snapshotToMermaid(filteredSnap, { currentHash: ch, direction: direction.value });
				const id = genSvgId();
				const { svg, bindFunctions } = await mermaid.render(id, source);
				mermaidSvg.value = svg;
				// Mermaid emits `<g>` elements with the bindFunctions hook to attach click handlers
				// AFTER the SVG is inserted into the DOM. We defer the bind to next tick so
				// `v-html` has settled first; otherwise bindFunctions targets nodes that aren't
				// in the document yet.
				setTimeout(() => {
					try {
						const host = /** @type {HTMLElement | null} */ (modalRef.value);
						if (host && bindFunctions) {
							bindFunctions(host);
						}
					} catch (e) {
						console.warn("Issue binding mermaid click handlers", e);
					}
				}, 0);
			} catch (e) {
				console.error("Issue rendering history graph:", e);
				errorText.value = String(e && /** @type {Error} */ (e).message ? /** @type {Error} */ (e).message : e);
			}
		};

		const onNodeClick = (hash) => {
			const snap = lastSnapshot.value;
			if (!snap) {
				return;
			}
			const node = snap.nodes?.[hash];
			if (!node) {
				return;
			}
			emit("restore", node.queryModelJson);
			// Close the modal after a restore — the user picked their target, no value in
			// keeping the picker on top of the now-restored view.
			emit("update:show", false);
		};

		const onForget = (hash) => {
			history.forget(hash);
			renderNow();
		};

		const onClearAll = () => {
			if (typeof window !== "undefined" && !window.confirm("Clear the entire query history for this cube? This cannot be undone.")) {
				return;
			}
			history.clearAll();
			renderNow();
		};

		const toggleDirection = () => {
			direction.value = direction.value === "TD" ? "LR" : "TD";
			if (lastSnapshot.value) {
				renderNow();
			}
		};

		/**
		 * Cycle a chip through the three states default → include → exclude → default.
		 * `kind` selects which axis (columns vs measures); two parallel Maps make the math
		 * trivial and keep their state independent (clearing all columns leaves measure
		 * filters intact).
		 */
		const cycleFilter = (kind, name) => {
			const map = kind === "measures" ? filterStates.measures : filterStates.columns;
			const cur = map.get(name);
			if (cur === undefined) {
				map.set(name, "include");
			} else if (cur === "include") {
				map.set(name, "exclude");
			} else {
				map.delete(name);
			}
			renderNow();
		};

		/** Reset every chip back to "default". */
		const clearFilters = () => {
			filterStates.columns.clear();
			filterStates.measures.clear();
			renderNow();
		};

		/**
		 * Bootstrap class set per chip state. Returned as a string for direct `:class` binding
		 * — keeps the template terse and centralises the visual mapping.
		 */
		const chipClasses = (state) => {
			if (state === "include") return "btn btn-sm btn-success py-0 px-2";
			if (state === "exclude") return "btn btn-sm btn-danger py-0 px-2 text-decoration-line-through";
			return "btn btn-sm btn-outline-secondary py-0 px-2";
		};

		const chipIcon = (state) => {
			if (state === "include") return "bi bi-check2 me-1";
			if (state === "exclude") return "bi bi-x me-1";
			return "";
		};

		// True when any chip is non-default — drives the "Clear filters" visibility.
		const hasActiveFilters = () => filterStates.columns.size > 0 || filterStates.measures.size > 0;

		onMounted(() => {
			if (!modalRef.value) return;
			bootstrapModal = new Modal(/** @type {HTMLElement} */ (modalRef.value), {});
			/** @type {HTMLElement} */ (modalRef.value).addEventListener("hidden.bs.modal", () => {
				emit("update:show", false);
			});
			// Register the per-instance click slot AND expose it under the fixed name that the
			// mermaid source emits ("onHistoryNodeClick"). The fixed name keeps the converter
			// pure; the generation slot survives concurrent modal mounts.
			/** @type {any} */
			const w = typeof window !== "undefined" ? window : {};
			w[clickFnName] = onNodeClick;
			w.onHistoryNodeClick = onNodeClick;
		});

		onBeforeUnmount(() => {
			/** @type {any} */
			const w = typeof window !== "undefined" ? window : {};
			delete w[clickFnName];
			// Only unset the shared name if it still points at THIS instance — a later modal that
			// remounted concurrently may have overwritten it, and we shouldn't strip its handler.
			if (w.onHistoryNodeClick === onNodeClick) {
				delete w.onHistoryNodeClick;
			}
		});

		watch(
			() => props.show,
			(next) => {
				if (!bootstrapModal) return;
				if (next) {
					bootstrapModal.show();
					renderNow();
				} else {
					bootstrapModal.hide();
				}
			},
		);

		// Re-render the open modal when a new query is captured upstream — keeps the view
		// in lock-step with the underlying store.
		watch(
			() => props.bumpVersion,
			() => {
				if (props.show) {
					renderNow();
				}
			},
		);

		// Format a node's summary for the list row. Compact column / measure pills.
		const summariseNode = (node) => {
			const cols = (node.columnNames || []).slice(0, 6);
			const meas = (node.measureNames || []).slice(0, 6);
			const more = Math.max(0, (node.columnNames?.length || 0) + (node.measureNames?.length || 0) - cols.length - meas.length);
			return {
				cols,
				meas,
				more,
				visitCount: node.visitCount || 1,
				lastSeenAt: node.lastSeenAt,
			};
		};

		return {
			mermaidSvg,
			errorText,
			modalRef,
			direction,
			toggleDirection,
			nodeList,
			currentHash,
			summariseNode,
			onRestoreClick: onNodeClick,
			onForget,
			onClearAll,
			// Chip-strip filtering surface.
			distinctNames,
			filterStates,
			cycleFilter,
			clearFilters,
			chipClasses,
			chipIcon,
			hasActiveFilters,
		};
	},
	template: /* HTML */ `
		<div class="modal fade" tabindex="-1" aria-labelledby="historyModalLabel" aria-hidden="true" :ref="(el) => (modalRef = el)">
			<div class="modal-dialog modal-dialog-centered modal-xl modal-fullscreen-lg-down">
				<div class="modal-content">
					<div class="modal-header">
						<h5 class="modal-title" id="historyModalLabel">
							<i class="bi bi-clock-history me-1"></i>
							Query history for this cube
						</h5>
						<div class="ms-auto d-flex gap-2 align-items-center">
							<button
								type="button"
								class="btn btn-sm btn-outline-secondary"
								@click="toggleDirection"
								:title="direction === 'TD' ? 'Switch to left-to-right layout' : 'Switch to top-down layout'"
							>
								<i :class="direction === 'TD' ? 'bi bi-arrow-down' : 'bi bi-arrow-right'"></i>
								{{ direction === "TD" ? "Top-down" : "Left-to-right" }}
							</button>
							<button
								type="button"
								class="btn btn-sm btn-outline-danger"
								@click="onClearAll"
								:disabled="nodeList.length === 0"
								title="Permanently wipe the persisted query history for this cube. Cannot be undone."
							>
								<i class="bi bi-trash"></i> Clear all
							</button>
							<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
						</div>
					</div>
					<div class="modal-body">
						<div v-if="errorText" class="alert alert-warning small">{{errorText}}</div>

						<!--
							Tri-state chip strip. Each chip cycles default → include → exclude on click:
							  - default (outline-secondary): no filter on this name.
							  - include (success): only nodes that REFERENCE this name pass.
							  - exclude (danger + strike-through): nodes that reference this name are HIDDEN.
							Columns and measures are kept in separate rows so the user reads them at a glance
							and the include / exclude semantics don't get mixed up. The clear-filters button
							resets every chip to default; only shown when at least one chip is non-default
							so the modal stays uncluttered for first-time users.
						-->
						<div v-if="distinctNames.columns.length > 0 || distinctNames.measures.length > 0" class="mb-3">
							<div class="d-flex justify-content-between align-items-center mb-1">
								<small class="text-muted">Filter: click a chip to include (✓) or exclude (✗)</small>
								<button
									v-if="hasActiveFilters()"
									type="button"
									class="btn btn-sm btn-link p-0 text-decoration-none"
									@click="clearFilters"
									title="Reset every chip back to default — show all queries again"
								>
									<i class="bi bi-x-circle me-1"></i>Clear filters
								</button>
							</div>
							<div v-if="distinctNames.columns.length > 0" class="d-flex flex-wrap align-items-center gap-1 mb-1">
								<small class="text-muted me-1" style="min-width: 60px;">Columns:</small>
								<button
									v-for="name in distinctNames.columns"
									:key="'col-' + name"
									type="button"
									:class="chipClasses(filterStates.columns.get(name))"
									style="font-size: 0.78rem;"
									@click="cycleFilter('columns', name)"
									:title="filterStates.columns.get(name) === 'include' ? 'Including — click to exclude' : filterStates.columns.get(name) === 'exclude' ? 'Excluding — click to reset' : 'Click to include'"
								>
									<i :class="chipIcon(filterStates.columns.get(name))"></i>#{{ name }}
								</button>
							</div>
							<div v-if="distinctNames.measures.length > 0" class="d-flex flex-wrap align-items-center gap-1">
								<small class="text-muted me-1" style="min-width: 60px;">Measures:</small>
								<button
									v-for="name in distinctNames.measures"
									:key="'mea-' + name"
									type="button"
									:class="chipClasses(filterStates.measures.get(name))"
									style="font-size: 0.78rem;"
									@click="cycleFilter('measures', name)"
									:title="filterStates.measures.get(name) === 'include' ? 'Including — click to exclude' : filterStates.measures.get(name) === 'exclude' ? 'Excluding — click to reset' : 'Click to include'"
								>
									<i :class="chipIcon(filterStates.measures.get(name))"></i>Σ{{ name }}
								</button>
							</div>
						</div>

						<!--
							Graph: visual catalogue of past queries. Edges carry the diff that took the user
							from one query to the next. The current node is highlighted. Click a node to
							restore — same affordance as the list rows below, for users who navigate visually.
						-->
						<div class="border rounded p-2 mb-3" style="overflow:auto; max-height: 50vh;">
							<pre class="mermaid" v-html="mermaidSvg" style="background: transparent; margin: 0;"></pre>
						</div>

						<!--
							List view: the discoverable interaction surface (click anywhere on a row to
							restore, "Forget" to drop, visit count + relative time as secondary info).
							Doubles as a textual fallback when mermaid fails to render or for accessibility
							tooling that cannot read SVG.
						-->
						<div v-if="nodeList.length === 0 &amp;&amp; hasActiveFilters()" class="text-muted text-center py-4">
							No query matches the active filter — adjust the chips above or
							<button type="button" class="btn btn-link p-0 align-baseline" @click="clearFilters">clear filters</button>.
						</div>
						<div v-else-if="nodeList.length === 0" class="text-muted text-center py-4">
							No history yet — run a query to start building your graph.
						</div>
						<div v-else>
							<h6 class="text-muted small text-uppercase mb-2">Past queries ({{nodeList.length}})</h6>
							<ul class="list-group">
								<!--
									Row layout: was a single restore-the-whole-row button. Refactored to a
									plain div so the per-column / per-measure chips can be REAL buttons
									(reused from the top strip via cycleFilter / chipClasses / chipIcon)
									without nesting interactive content inside an outer button — which was
									HTML-invalid and announced poorly to screen readers. Restore is now an
									explicit affordance on the right, next to Forget.
								-->
								<li
									v-for="node in nodeList"
									:key="node.id"
									class="list-group-item d-flex gap-2 align-items-center flex-wrap"
									:class="node.id === currentHash ? 'border-info border-2' : ''"
								>
									<div class="flex-grow-1 d-flex flex-wrap align-items-center gap-1">
										<span v-if="node.id === currentHash" class="badge bg-info-subtle text-info-emphasis">current</span>
										<!--
											Per-column chip. Same widget + same tri-state semantics as the top strip.
											Click cycles default → include → exclude for THIS column across the modal
											(both the graph and the list update). The chip does NOT restore the row —
											restore is now an explicit Restore button on the right.
										-->
										<button
											v-for="c in summariseNode(node).cols"
											:key="'c-' + c"
											type="button"
											:class="chipClasses(filterStates.columns.get(c))"
											style="font-size: 0.72rem;"
											@click="cycleFilter('columns', c)"
											:title="filterStates.columns.get(c) === 'include' ? 'Including ' + c + ' — click to exclude' : filterStates.columns.get(c) === 'exclude' ? 'Excluding ' + c + ' — click to reset' : 'Click to include ' + c"
										>
											<i :class="chipIcon(filterStates.columns.get(c))"></i>#{{ c }}
										</button>
										<button
											v-for="m in summariseNode(node).meas"
											:key="'m-' + m"
											type="button"
											:class="chipClasses(filterStates.measures.get(m))"
											style="font-size: 0.72rem;"
											@click="cycleFilter('measures', m)"
											:title="filterStates.measures.get(m) === 'include' ? 'Including ' + m + ' — click to exclude' : filterStates.measures.get(m) === 'exclude' ? 'Excluding ' + m + ' — click to reset' : 'Click to include ' + m"
										>
											<i :class="chipIcon(filterStates.measures.get(m))"></i>Σ{{ m }}
										</button>
										<span v-if="summariseNode(node).more > 0" class="text-muted small">+{{summariseNode(node).more}} more</span>
										<span
											v-if="summariseNode(node).cols.length === 0 &amp;&amp; summariseNode(node).meas.length === 0"
											class="text-muted fst-italic"
											>empty query</span
										>
										<small class="text-muted ms-2"
											>·{{summariseNode(node).visitCount}} visit{{summariseNode(node).visitCount > 1 ? 's' : ''}}</small
										>
									</div>
									<!--
										Restore: explicit button on the right. Disabled on the currently-selected
										row so we don't restore-to-where-you-already-are (mildly confusing — the
										row would close the modal even though nothing changed).
									-->
									<button
										type="button"
										class="btn btn-sm btn-outline-primary flex-shrink-0"
										@click="onRestoreClick(node.id)"
										:disabled="node.id === currentHash"
										:title="node.id === currentHash ? 'This is the currently-displayed query' : 'Restore this query'"
									>
										<i class="bi bi-arrow-counterclockwise me-1"></i>Restore
									</button>
									<button
										type="button"
										class="btn btn-sm btn-outline-secondary flex-shrink-0"
										@click="onForget(node.id)"
										:disabled="node.id === currentHash"
										:title="node.id === currentHash ? 'Cannot forget the currently-displayed query' : 'Permanently drop this query from your history'"
									>
										<i class="bi bi-x-lg"></i>
									</button>
								</li>
							</ul>
						</div>
					</div>
				</div>
			</div>
		</div>
	`,
};

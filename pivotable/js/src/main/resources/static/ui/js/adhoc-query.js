// @ts-check
import { computed, reactive, ref, watch, provide, onMounted, onUnmounted } from "vue";

import { Collapse } from "bootstrap";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import queryHelper from "./adhoc-query-helper.js";
import { aggregateNamesByKind, useQueryHistoryStore } from "./adhoc-query-history-store.js";

// Endpoint + cube headers are now shown in the navbar breadcrumb (see `adhoc-navbar.js`),
// so neither AdhocEndpointHeader nor AdhocCubeHeader is mounted on the query page anymore.

import { useUserStore } from "./store-user.js";
import { usePreferencesStore } from "./store-preferences.js";

import AdhocQueryWizard from "./adhoc-query-wizard.js";
import AdhocQueryExecutor from "./adhoc-query-executor.js";
import AdhocQueryGrid from "./adhoc-query-grid.js";

import { useRouter } from "vue-router";

import AdhocMeasuresDag from "./adhoc-measures-dag.js";
import AdhocQueryWizardColumnFilterModalSingleton from "./adhoc-query-wizard-column-filter-modal-singleton.js";
import AdhocQueryChatbot from "./adhoc-query-chatbot.js";
import AdhocQueryPlanLive from "./adhoc-query-plan-live.js";

import { defaultExecutorBus } from "./adhoc-executor-bus.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocQueryWizard,
		AdhocQueryExecutor,
		AdhocQueryGrid,

		AdhocMeasuresDag,
		AdhocQueryWizardColumnFilterModalSingleton,
		AdhocQueryChatbot,
		AdhocQueryPlanLive,
	},
	// https://vuejs.org/guide/components/props.html
	props: {
		cubeId: {
			type: String,
			required: true,
		},
		endpointId: {
			type: String,
			required: true,
		},

		cube: {
			type: Object,
			required: true,
		},
	},
	computed: {
		...mapState(useUserStore, ["needsToLogin"]),
		...mapState(useAdhocStore, ["nbSchemaFetching"]),
		...mapState(useAdhocStore, {
			endpoint(store) {
				const self = /** @type {any} */ (this);
				return store.endpoints[self.endpointId] || { error: "not_loaded" };
			},
			schema(store) {
				const self = /** @type {any} */ (this);
				return store.schemas[self.endpointId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadCubeSchemaIfMissing(props.cubeId, props.endpointId);

		const loading = ref(false);
		/** @type {any} reactive query model — methods (`onColumnToggled`, …) are bolted on later via `Object.assign` */
		const queryModel = reactive(queryHelper.makeQueryModel());

		// Watch for changes on `selectedColumns` to update `selectedColumnsOrdered` accordingly
		watch(
			() => queryModel.selectedColumns,
			(newX) => {
				queryModel.onColumnToggled();
			},
			{ deep: true },
		);

		// https://vuejs.org/guide/components/provide-inject.html
		provide("queryModel", queryModel);
		provide("cube", props.cube);

		const measuresDagModel = reactive({
			main: "",
			highlight: [],
		});
		provide("measuresDagModel", measuresDagModel);

		const columnFilterModel = reactive({
			column: "",
		});
		// https://vuejs.org/guide/components/provide-inject.html
		provide("columnFilterModel", columnFilterModel);

		// Shared model driving the per-measure Statistics modal. Header buttons in the
		// grid set `measureName` + `stats` and trigger the modal via Bootstrap; the modal
		// reads the same singleton object. Living here (next to the other singleton
		// modal models) keeps the wiring symmetric across cellModal / measureDag /
		// columnFilter / measureStats.
		const measureStatsModel = reactive({
			measureName: "",
			stats: null,
		});
		provide("measureStatsModel", measureStatsModel);

		/** @type {any} reactive container — `view`, `error`, `timing`, `loading` are filled in by the executor */
		const tabularView = reactive({});

		// Shared reactive flag indicating whether any wizard accordion (columns / measures /
		// custom markers / options) is currently expanded. The accordion's `data-bs-parent`
		// enforces at-most-one-open, so `isOpen` is effectively "is any body visible".
		//
		// Consumer: the executor floats its Submit block over the grid when `isOpen` is true,
		// so the button stays visible without requiring the user to scroll past a tall open
		// accordion. When the user collapses the accordion (or clicks Submit, which also
		// collapses), the Submit block returns to its normal position below the wizard.
		const accordionState = reactive({ isOpen: false });
		provide("accordionState", accordionState);

		// Shared "executor live state" bag. Owned by this component (the common ancestor of the
		// executor and the grid). The executor — receiving `executorBus` as a prop — writes its
		// live state into this object (`isQueryInFlight`, `isSameAsLastQuery`, `autoQuery`,
		// `submitQuery`). The grid-controls reads it back via inject("executorBus"). The bus
		// MUST live here because the grid is a sibling of the executor, so provide/inject in the
		// executor itself cannot reach the grid subtree. Keep this fact in the comment — past
		// versions tried provide() in the executor and got an always-truthy inject default,
		// which made the Refresh button render a permanent "Refreshing…" spinner.
		const executorBus = reactive(defaultExecutorBus());
		provide("executorBus", executorBus);

		// Bootstrap 5 dispatches native CustomEvents that bubble, so one document-level listener
		// covers every accordion inside the wizard. We scope via `closest("#accordionWizard")`
		// to avoid reacting to other collapses on the page (login modal etc.).
		//
		// We listen to `show.bs.collapse` / `hide.bs.collapse` (the "start of animation"
		// events, fired BEFORE Bootstrap animates) rather than `shown` / `hidden` (fired AFTER).
		// This way the Submit block's own transform/fade animation runs IN PARALLEL with the
		// Bootstrap collapse animation — instead of waiting for it to finish, which visibly
		// doubled the perceived delay.
		const onAccordionShow = (event) => {
			if (event.target && event.target.closest && event.target.closest("#accordionWizard")) {
				accordionState.isOpen = true;
				// Persist which section was opened so an F5 brings the user back to the
				// same place. `event.target` is the `.accordion-collapse` div, whose id is
				// e.g. `wizardColumns` / `wizardMeasures` / `wizardCustoms` / `wizardOptions`.
				if (event.target.id) {
					preferencesStore.wizardOpenAccordion = event.target.id;
				}
			}
		};
		const onAccordionHide = (event) => {
			if (event.target && event.target.closest && event.target.closest("#accordionWizard")) {
				// Mirror behaviour: when an accordion starts hiding, set isOpen=false so the
				// Submit block's leave animation starts right away. The `data-bs-parent` pattern
				// enforces at-most-one-open, so we don't need to check for other-still-open
				// siblings here.
				accordionState.isOpen = false;
				// Clear the persisted open-section ONLY if the section that's hiding is the
				// one we previously persisted. Otherwise a Bootstrap data-bs-parent close-on-
				// open of a sibling would erase the new open id we just wrote in onShow.
				if (event.target.id && preferencesStore.wizardOpenAccordion === event.target.id) {
					preferencesStore.wizardOpenAccordion = "";
				}
			}
		};
		// Click-on-grid → collapse any open wizard accordion. The Submit block re-docks under the
		// wizard automatically because it watches `accordionState.isOpen`. Without this, the user has
		// to either toggle the accordion header again or click outside both panels, which is unintuitive
		// after they've started interacting with the grid. We detect "click on the grid" via a delegated
		// listener that looks for a `.slick-viewport` ancestor (SlickGrid's scroll container — covers
		// header click, cell click, and resize-handle dblclick alike).
		const onDocumentClick = (event) => {
			if (!event.target || typeof event.target.closest !== "function") {
				return;
			}
			// Only fire when the click was inside the SlickGrid. `.slick-viewport` is SlickGrid's
			// canonical scroll-container class, present on every running build.
			if (!event.target.closest(".slick-viewport")) {
				return;
			}
			// Find the open accordion panel (Bootstrap stamps `.show` on the active `.accordion-collapse`).
			const open = document.querySelector("#accordionWizard .accordion-collapse.show");
			if (!open) {
				return;
			}
			// Hide via Bootstrap's Collapse API so the standard `hide.bs.collapse` event fires —
			// which our `onAccordionHide` listener uses to flip `accordionState.isOpen` and let the
			// Submit block re-dock.
			Collapse.getOrCreateInstance(open).hide();
		};

		onMounted(() => {
			document.addEventListener("show.bs.collapse", onAccordionShow);
			document.addEventListener("hide.bs.collapse", onAccordionHide);
			document.addEventListener("click", onDocumentClick);

			// Restore the last-open accordion. The wizard is rendered after this hook
			// fires, so we wait one tick for the DOM. We toggle by adding the `show`
			// class directly + flipping the trigger button's aria-expanded, mirroring
			// Bootstrap's own collapse state — going through Bootstrap's JS Collapse API
			// would also fire animations, which here we don't need (we're restoring an
			// already-final state, not transitioning to it).
			const savedId = preferencesStore.wizardOpenAccordion;
			if (savedId) {
				requestAnimationFrame(() => {
					const panel = document.getElementById(savedId);
					if (!panel) return;
					panel.classList.add("show");
					const trigger = document.querySelector('[data-bs-target="#' + savedId + '"]');
					if (trigger) {
						trigger.classList.remove("collapsed");
						trigger.setAttribute("aria-expanded", "true");
					}
					accordionState.isOpen = true;
				});
			}
		});
		onUnmounted(() => {
			document.removeEventListener("show.bs.collapse", onAccordionShow);
			document.removeEventListener("hide.bs.collapse", onAccordionHide);
			document.removeEventListener("click", onDocumentClick);
		});

		// Snapshot of the queryModel as it was when the last successful result landed. Used by the
		// "query is broken — restore last successful" banner so the user can roll back after e.g.
		// adding a measure that always throws. The snapshot is a plain JSON (via queryModelToParsedJson),
		// not a live reference to the reactive queryModel.
		const lastSuccessfulQuery = ref(null);

		// Per-cube, persistent (localStorage) history graph. Phase 2: the wizard pickers
		// consume the aggregated `historyScores` (computed below) to rank personally-frequented
		// columns / measures higher within their text-match tier. The store accumulates one
		// node per distinct executed query (content-hash dedup) and one edge per transition.
		// See `adhoc-query-history-store.js` for the design.
		const history = useQueryHistoryStore(props.cubeId);

		// Reactive trigger for the computed `historyScores` below: incremented every time a
		// new query is captured so consumers re-aggregate without us threading reactivity
		// through the framework-agnostic store module. Starts at 0 — the initial `historyScores`
		// computation already picks up whatever cumulative state is in localStorage.
		const historyBump = ref(0);

		watch(
			() => tabularView.view,
			(newView) => {
				if (newView) {
					const snapshot = queryHelper.queryModelToParsedJson(queryModel);
					lastSuccessfulQuery.value = snapshot;
					// Capture executes-only: typing mid-edit in a filter modal never lands in the
					// graph; only queries the user actually ran do. Defensive try/catch — the
					// history store is a non-essential side cache and must not break the query
					// flow if its localStorage backing trips on quota or shape corruption.
					try {
						history.recordExecutedQuery(snapshot);
						historyBump.value++;
					} catch (e) {
						console.warn("Failed to record query into history graph", e);
					}
				}
			},
		);

		// Aggregated personal-history scores per (kind, name), recomputed when `historyBump`
		// ticks. Passed to the wizard so its filtered() pipeline uses them as a secondary
		// sort key — items the user has touched float to the top of their text-match tier.
		// The `void historyBump.value` access creates the reactive dep without using the
		// value; Vue tracks it and re-runs the computed on every increment.
		const historyScores = computed(() => {
			void historyBump.value;
			const snap = history.snapshot();
			return {
				columns: aggregateNamesByKind(snap, "column"),
				measures: aggregateNamesByKind(snap, "measure"),
				filterColumns: aggregateNamesByKind(snap, "filterColumn"),
			};
		});

		const restoreLastSuccessfulQuery = function () {
			if (!lastSuccessfulQuery.value) {
				return;
			}
			// parsedJsonToQueryModel resets before populating — full snapshot replacement, so the faulty
			// measure/column/filter that caused the failure is dropped cleanly.
			queryHelper.parsedJsonToQueryModel(lastSuccessfulQuery.value, queryModel);
			// Clear the error immediately. A successful re-query will re-clear it; clearing here too
			// avoids a flash of the banner while the new query is in flight.
			tabularView.error = "";
			tabularView.errorStack = null;
		};

		const router = useRouter();

		// https://github.com/vuejs/router/issues/2017
		// Even with `router.isReady()`, `currentRoute.value.hash` can be STALE on the remount
		// path (e.g. after a token-expiry + in-place re-login that swapped <LoginChip> back to
		// <AdhocQuery>). The model→URL watcher below writes the hash via `history.pushState`,
		// which bypasses vue-router; vue-router's reactive `currentRoute.value.hash` therefore
		// stops tracking the real URL after the first edit. Read straight from
		// `window.location.hash` via the helper so both hydration AND every subsequent compare
		// use the authoritative source (window.location).
		/** @type {((event: PopStateEvent) => void) | null} */
		let popStateListener = null;
		router.isReady().then(() => {
			const currentHashDecoded = queryHelper.readUrlHash();

			queryHelper.hashToQueryModel(currentHashDecoded, queryModel);

			// Save queryModel into URL hash — each edit becomes a new history entry so
			// the browser back button returns to the previous view.
			//
			// The hash-equality guard serves two purposes:
			//   1. Breaks the feedback loop between this watcher and the popstate
			//      listener below — when popstate restores queryModel from the URL,
			//      the re-encoded hash matches the URL and we skip pushState.
			//   2. Avoids duplicate history entries when a mutation leaves the
			//      hash semantically unchanged.
			watch(queryModel, async (newQueryModel) => {
				// `window.location.hash` (via readUrlHash) is the authoritative source — see the
				// staleness note above. `currentRoute.value.hash` would diverge after the first
				// pushState and silently drop top-level hash fields like `v` on the next compare.
				const currentHashDecoded = queryHelper.readUrlHash();

				const newHash = queryHelper.queryModelToHash(currentHashDecoded, newQueryModel);

				if (newHash === currentHashDecoded) {
					return;
				}

				// https://stackoverflow.com/questions/51337255/silently-update-url-without-triggering-route-in-vue-router
				const newUrl = router.currentRoute.value.path + newHash;

				history.pushState({}, null, newUrl);
			});

			// Browser back/forward: subscribe to the NATIVE popstate event rather than
			// vue-router's reactive `currentRoute.value.hash`. The reactive hash desyncs
			// once we start writing via `history.pushState` (see staleness note above) —
			// `popstate` is the browser's authoritative signal and fires for every back/
			// forward, regardless of whether the underlying entry was pushed by vue-router
			// or by our raw `history.pushState` calls. Re-reads from `window.location.hash`
			// via the same helper used at hydration so behaviour is symmetric.
			//
			// TODO Roadmap: this re-triggers a full query recomputation on every
			// back/forward. A future improvement could cache previously-computed
			// TabularViews by hash to restore instantly without a round-trip
			// (at the cost of showing potentially stale data).
			popStateListener = () => {
				queryHelper.hashToQueryModel(queryHelper.readUrlHash(), queryModel);
			};
			window.addEventListener("popstate", popStateListener);
		});

		// Remove the popstate listener when this component unmounts — otherwise an
		// in-place remount (e.g. token-expiry + re-login) would stack a second listener
		// on top of the first and run hashToQueryModel twice per back/forward click.
		onUnmounted(() => {
			if (popStateListener) {
				window.removeEventListener("popstate", popStateListener);
				popStateListener = null;
			}
		});

		// SlickGrid requires a cssSelector
		const domId = ref("slickgrid_" + Math.floor(Math.random() * 1024));
		console.log("SlickGrid id is", "#" + domId.value);

		// Full-screen-grid mode. When `preferencesStore.wizardHidden`, the left-column wizard
		// is hidden and the grid column expands to full width. Only the class bindings live
		// here — the toggle BUTTON is rendered inside the grid component, next to the other
		// grid-level controls (Export CSV, Formatting Options). Persisted via
		// `store-preferences.js:buildPayload`, so the preference survives page reloads.
		const preferencesStore = usePreferencesStore();

		// Shared model populated by AdhocQueryGrid and consumed by the LiveView strip below.
		// `offscreenColumnsRight` is the number of data columns whose left edge sits past the
		// visible viewport's right edge in scroll mode; `scrollToRightEnd` is the action assigned
		// by the grid after mount, invoked by the chip click. Centralising the model here lets
		// the chip render in the same row as <AdhocQueryPlanLive> instead of being trapped inside
		// the grid container.
		const gridShared = reactive({
			offscreenColumnsRight: 0,
			/** @type {(() => void) | null} */
			scrollToRightEnd: null,
		});
		const onScrollToRightEnd = () => {
			if (gridShared.scrollToRightEnd) gridShared.scrollToRightEnd();
		};

		return {
			loading,
			queryModel,
			historyScores,
			tabularView,
			domId,

			measuresDagModel,
			columnFilterModel,

			lastSuccessfulQuery,
			restoreLastSuccessfulQuery,

			preferencesStore,

			gridShared,
			onScrollToRightEnd,

			executorBus,
		};
	},
	template: /* HTML */ `
		<!--
			Endpoint + cube identification moved to the navbar breadcrumb (Endpoints / endpoint
			/ cube). Skipping the body-side header gives the grid the extra vertical real
			estate the user asked for. The previous AdhocCubeHeader mount is intentionally
			gone — its information is now visible at all times in the top bar.
		-->
		<div class="row">
			<!--
				Wizard column. Hidden when preferencesStore.wizardHidden is true — grid below
				then expands to col-12 for a "full-screen grid" mode. The toggle button lives in
				the grid column so it remains reachable even when the wizard is hidden.
			-->
			<div :class="preferencesStore.wizardHidden ? 'd-none' : 'col-3'">
				<div class="row">
					<AdhocQueryWizard :endpointId="endpointId" :cubeId="cubeId" :queryModel="queryModel" :historyScores="historyScores" :loading="loading" />
				</div>

				<div class="row">
					<AdhocQueryExecutor
						:endpointId="endpointId"
						:cubeId="cubeId"
						:queryModel="queryModel"
						:tabularView="tabularView"
						:loading="loading"
						:executorBus="executorBus"
					/>
				</div>
			</div>
			<div :class="preferencesStore.wizardHidden ? 'col-12' : 'col-9'">
				<!--
					Prominent "query broken" banner. Sticky so it stays visible as the user scrolls the grid.
					The grid below intentionally keeps rendering the last successful view to preserve the user's
					mental context; this banner makes it impossible to miss that the underlying state is out-of-sync
					with the displayed data.
				-->
				<div v-if="tabularView.error" class="alert alert-danger sticky-top mb-2" role="alert">
					<div class="d-flex justify-content-between align-items-start">
						<div class="flex-grow-1">
							<strong>Query is broken.</strong>
							The grid below still shows the last successful result.
							<div class="small">{{tabularView.error}}</div>
							<!--
								Full server-side stack on demand. The <details> element keeps it tucked away by
								default — the short message in .small above is enough for most users — and the
								monospace <pre> preserves indentation when expanded. We disable text wrapping with
								white-space:pre-wrap so long class names stay readable.
							-->
							<details v-if="tabularView.errorStack" class="mt-2">
								<summary class="small text-decoration-underline" style="cursor:pointer">Server stack trace</summary>
								<pre class="small mt-1 mb-0" style="white-space:pre-wrap;max-height:20rem;overflow:auto">{{tabularView.errorStack}}</pre>
							</details>
						</div>
						<button
							v-if="lastSuccessfulQuery"
							type="button"
							class="btn btn-sm btn-outline-dark ms-3"
							@click="restoreLastSuccessfulQuery"
							title="Drop the latest edits and restore the queryModel as it was for the last successful query"
						>
							Restore last successful query
						</button>
					</div>
				</div>
				<div v-if="tabularView.queryUuid" class="mt-2 mb-1 d-flex justify-content-between align-items-center gap-2">
					<AdhocQueryPlanLive :queryUuid="tabularView.queryUuid" />
					<!--
						Scroll-mode discoverability hint: sits on the right of the LiveView strip so
						it stays visible regardless of how far the user scrolled the table. The chip
						is owned here (not inside the grid) so it doesn't get clipped by the grid's
						overflow-x and remains aligned with the rest of the page chrome. The grid
						populates gridShared after mount (count + the scroll-right action).
					-->
					<button
						type="button"
						v-if="gridShared.offscreenColumnsRight > 0"
						@click="onScrollToRightEnd"
						class="btn btn-sm btn-dark py-0 px-2"
						style="font-size:0.75rem;"
						:title="'Scroll the grid to reveal ' + gridShared.offscreenColumnsRight + ' column(s) hidden to the right'"
					>
						<i class="bi bi-arrow-right-circle"></i>
						+{{gridShared.offscreenColumnsRight}} more
					</button>
				</div>
				<AdhocQueryGrid
					:tabularView="tabularView"
					:loading="loading"
					:queryModel="queryModel"
					:domId="domId"
					:cube="cube"
					:endpointId="endpointId"
					:cubeId="cubeId"
					:gridShared="gridShared"
				/>
			</div>

			<AdhocMeasuresDag :measuresDagModel="measuresDagModel" />
			<AdhocQueryWizardColumnFilterModalSingleton :columnFilterModel="columnFilterModel" />
			<AdhocQueryChatbot :endpointId="endpointId" :cubeId="cubeId" />
		</div>
	`,
};

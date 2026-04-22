import { reactive, ref, watch, provide, onMounted, onUnmounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import queryHelper from "./adhoc-query-helper.js";

import AdhocEndpointHeader from "./adhoc-endpoint-header.js";
import AdhocCubeHeader from "./adhoc-cube-header.js";

import { useUserStore } from "./store-user.js";

import AdhocQueryWizard from "./adhoc-query-wizard.js";
import AdhocQueryExecutor from "./adhoc-query-executor.js";
import AdhocQueryGrid from "./adhoc-query-grid.js";

import { useRouter } from "vue-router";

import AdhocMeasuresDag from "./adhoc-measures-dag.js";
import AdhocQueryWizardColumnFilterModalSingleton from "./adhoc-query-wizard-column-filter-modal-singleton.js";
import AdhocQueryChatbot from "./adhoc-query-chatbot.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocEndpointHeader,
		AdhocCubeHeader,
		AdhocQueryWizard,
		AdhocQueryExecutor,
		AdhocQueryGrid,

		AdhocMeasuresDag,
		AdhocQueryWizardColumnFilterModalSingleton,
		AdhocQueryChatbot,
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
				return store.endpoints[this.endpointId] || { error: "not_loaded" };
			},
			schema(store) {
				return store.schemas[this.endpointId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadCubeSchemaIfMissing(props.cubeId, props.endpointId);

		const loading = ref(false);
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
			}
		};
		const onAccordionHide = (event) => {
			if (event.target && event.target.closest && event.target.closest("#accordionWizard")) {
				// Mirror behaviour: when an accordion starts hiding, set isOpen=false so the
				// Submit block's leave animation starts right away. The `data-bs-parent` pattern
				// enforces at-most-one-open, so we don't need to check for other-still-open
				// siblings here.
				accordionState.isOpen = false;
			}
		};
		onMounted(() => {
			document.addEventListener("show.bs.collapse", onAccordionShow);
			document.addEventListener("hide.bs.collapse", onAccordionHide);
		});
		onUnmounted(() => {
			document.removeEventListener("show.bs.collapse", onAccordionShow);
			document.removeEventListener("hide.bs.collapse", onAccordionHide);
		});

		// Snapshot of the queryModel as it was when the last successful result landed. Used by the
		// "query is broken — restore last successful" banner so the user can roll back after e.g.
		// adding a measure that always throws. The snapshot is a plain JSON (via queryModelToParsedJson),
		// not a live reference to the reactive queryModel.
		const lastSuccessfulQuery = ref(null);

		watch(
			() => tabularView.view,
			(newView) => {
				if (newView) {
					lastSuccessfulQuery.value = queryHelper.queryModelToParsedJson(queryModel);
				}
			},
		);

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
		};

		const router = useRouter();

		// https://github.com/vuejs/router/issues/2017
		// Else, typicall when re-logging-in without F5, we observe the hash may not be available through the router
		router.isReady().then(() => {
			const currentHashDecoded = router.currentRoute.value.hash;

			queryHelper.hashToQueryModel(currentHashDecoded, queryModel);

			// Save queryModel into URL hash — each edit becomes a new history entry so
			// the browser back button returns to the previous view.
			//
			// The hash-equality guard serves two purposes:
			//   1. Breaks the feedback loop between this watcher and the popstate
			//      watcher below — when popstate restores queryModel from the URL,
			//      the re-encoded hash matches the URL and we skip pushState.
			//   2. Avoids duplicate history entries when a mutation leaves the
			//      hash semantically unchanged.
			watch(queryModel, async (newQueryModel) => {
				const currentHashDecoded = router.currentRoute.value.hash;

				const newHash = queryHelper.queryModelToHash(currentHashDecoded, newQueryModel);

				if (newHash === currentHashDecoded) {
					return;
				}

				// https://stackoverflow.com/questions/51337255/silently-update-url-without-triggering-route-in-vue-router
				const newUrl = router.currentRoute.value.path + newHash;

				history.pushState({}, null, newUrl);
			});

			// Browser back/forward: vue-router updates `currentRoute.value.hash` on
			// popstate, and we reflect that change into queryModel, which re-runs
			// the query through the normal reactive pipeline.
			//
			// TODO Roadmap: this re-triggers a full query recomputation on every
			// back/forward. A future improvement could cache previously-computed
			// TabularViews by hash to restore instantly without a round-trip
			// (at the cost of showing potentially stale data).
			watch(
				() => router.currentRoute.value.hash,
				(newHash) => {
					queryHelper.hashToQueryModel(newHash, queryModel);
				},
			);
		});

		// TODO This structure should be persisted in localStorage
		const recentlyUsed = reactive({ columns: new Set(), measures: new Set() });
		{
			watch(queryModel, async (newQueryModel) => {
				const columns = Object.keys(newQueryModel.selectedColumns || {});
				// https://stackoverflow.com/questions/50881453/how-to-add-an-array-of-values-to-a-set
				columns.forEach(recentlyUsed.columns.add, recentlyUsed.columns);

				const measures = Object.keys(queryModel.selectedMeasures || {});
				// https://stackoverflow.com/questions/50881453/how-to-add-an-array-of-values-to-a-set
				measures.forEach(recentlyUsed.measures.add, recentlyUsed.measures);
			});
		}

		// SlickGrid requires a cssSelector
		const domId = ref("slickgrid_" + Math.floor(Math.random() * 1024));
		console.log("SlickGrid id is", "#" + domId.value);

		return {
			loading,
			queryModel,
			recentlyUsed,
			tabularView,
			domId,

			measuresDagModel,
			columnFilterModel,

			lastSuccessfulQuery,
			restoreLastSuccessfulQuery,
		};
	},
	template: /* HTML */ `
		<AdhocCubeHeader :endpointId="endpointId" :cubeId="cubeId" />
		<div class="row">
			<div class="col-3">
				<div class="row">
					<AdhocQueryWizard :endpointId="endpointId" :cubeId="cubeId" :queryModel="queryModel" :recentlyUsed="recentlyUsed" :loading="loading" />
				</div>

				<div class="row">
					<AdhocQueryExecutor :endpointId="endpointId" :cubeId="cubeId" :queryModel="queryModel" :tabularView="tabularView" :loading="loading" />
				</div>
			</div>
			<div class="col-9">
				<!--
					Prominent "query broken" banner. Sticky so it stays visible as the user scrolls the grid.
					The grid below intentionally keeps rendering the last successful view to preserve the user's
					mental context; this banner makes it impossible to miss that the underlying state is out-of-sync
					with the displayed data.
				-->
				<div v-if="tabularView.error" class="alert alert-danger d-flex justify-content-between align-items-center sticky-top mb-2" role="alert">
					<div>
						<strong>Query is broken.</strong>
						The grid below still shows the last successful result.
						<div class="small">{{tabularView.error}}</div>
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
				<AdhocQueryGrid :tabularView="tabularView" :loading="loading" :queryModel="queryModel" :domId="domId" :cube="cube" />
			</div>

			<AdhocMeasuresDag :measuresDagModel="measuresDagModel" />
			<AdhocQueryWizardColumnFilterModalSingleton :columnFilterModel="columnFilterModel" />
			<AdhocQueryChatbot :endpointId="endpointId" :cubeId="cubeId" />
		</div>
	`,
};

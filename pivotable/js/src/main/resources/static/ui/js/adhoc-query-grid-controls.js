// @ts-check
import { inject } from "vue";

import AdhocGridFormatModal from "./adhoc-query-grid-format-modal.js";
import AdhocGridExportCsv from "./adhoc-query-grid-export-csv.js";

import { usePreferencesStore } from "./store-preferences.js";
import { defaultExecutorBus } from "./adhoc-executor-bus.js";

// Grid-level control strip shown at the bottom of the grid column: Export dropdown, Formatting
// Options modal, and the full-screen-grid toggle (hide the wizard to let the grid use the full
// viewport width). Extracted from `adhoc-query-grid.js` so that file can focus on SlickGrid
// lifecycle rather than unrelated UI chrome.
//
// Full-screen mode is backed by `preferencesStore.wizardHidden` (persisted across reloads via
// the localStorage snapshot in `store-preferences.js`). The layout bindings in
// `adhoc-query.js` react to that same flag — this component only toggles it.
export default {
	components: {
		AdhocGridFormatModal,
		AdhocGridExportCsv,
	},
	props: {
		// Flat array of row objects; forwarded as-is to the Export CSV component.
		dataArray: {
			type: Array,
			required: true,
		},
		// Reactive format options bag; forwarded as-is to the Formatting modal.
		formatOptions: {
			type: Object,
			required: true,
		},
	},
	setup() {
		const preferencesStore = usePreferencesStore();
		const toggleWizardHidden = function () {
			preferencesStore.wizardHidden = !preferencesStore.wizardHidden;
		};
		// Toggle the grid's layout mode. Default is `fit` (columns sum to the viewport width); `scroll`
		// lets SlickGrid auto-size columns against their headers and exposes a horizontal scroll bar
		// when the natural sum exceeds the viewport. Persisted in `store-preferences.js` so the choice
		// survives reloads. Picked up by `adhoc-query-grid.js` on every resync.
		const toggleGridLayout = function () {
			preferencesStore.gridLayout = preferencesStore.gridLayout === "scroll" ? "fit" : "scroll";
		};
		// Provided by AdhocQuery (the common ancestor of the executor and the grid). The executor
		// writes its live state into this reactive bag; we read it back here. The bag is a single
		// object rather than four separate provides because:
		//  - provide/inject only flows down to descendants, so the executor cannot provide directly
		//    (the grid is a sibling, not a descendant). The parent owns the bus.
		//  - a plain object with reactive properties unwraps naturally in templates without ref/.value
		//    juggling, and the default below is a safe shape (all flags false, no-op submit) — so the
		//    component still renders correctly if mounted outside the AdhocQuery scope.
		const executorBus = inject("executorBus", defaultExecutorBus());
		return {
			preferencesStore,
			toggleWizardHidden,
			toggleGridLayout,
			executorBus,
		};
	},
	template: /* HTML */ `
		<div class="d-flex flex-wrap gap-2 align-items-center mt-2">
			<AdhocGridExportCsv :array="dataArray" />
			<AdhocGridFormatModal :formatOptions="formatOptions" />
			<!--
				Refresh button — mirrors the in-wizard Submit. Only rendered when the wizard is hidden (the
				actual Submit lives inside the wizard column, so the user would lose the affordance in full-
				screen-grid mode without this). Disabled while a query is in flight: re-clicking would just
				queue an identical request — the spinner is the right signal here.
			-->
			<button
				v-if="preferencesStore.wizardHidden"
				type="button"
				class="btn btn-outline-primary btn-sm"
				:class="executorBus.isQueryInFlight ? 'adhoc-busy' : ''"
				@click="executorBus.submitQuery"
				:disabled="executorBus.isQueryInFlight"
				:title="executorBus.isQueryInFlight ? 'A query is already running' : 'Re-run the current query'"
			>
				<span v-if="executorBus.isQueryInFlight">
					<span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>
					{{ executorBus.isSameAsLastQuery ? "Refreshing…" : "Querying…" }}
				</span>
				<span v-else><i class="bi bi-arrow-clockwise me-1"></i> Refresh</span>
			</button>
			<button
				type="button"
				class="btn btn-outline-secondary btn-sm"
				@click="toggleWizardHidden"
				:title="preferencesStore.wizardHidden ? 'Show the wizard (exit full-screen grid)' : 'Hide the wizard (full-screen grid)'"
			>
				<i :class="preferencesStore.wizardHidden ? 'bi bi-arrows-angle-contract me-1' : 'bi bi-arrows-fullscreen me-1'"></i>
				{{ preferencesStore.wizardHidden ? "Show wizard" : "Hide wizard" }}
			</button>
			<!--
				Layout toggle: 'fit' (default — columns fill viewport, no horizontal scroll) versus
				'scroll' (columns auto-size against headers, horizontal scroll appears when content
				overflows). Useful when long column names get truncated under 'fit'.
			-->
			<button
				type="button"
				class="btn btn-outline-secondary btn-sm"
				@click="toggleGridLayout"
				:title="
					preferencesStore.gridLayout === 'scroll'
						? 'Fit columns to viewport (no horizontal scroll)'
						: 'Scroll horizontally with auto-sized columns'
				"
			>
				<i :class="preferencesStore.gridLayout === 'scroll' ? 'bi bi-arrow-bar-left me-1' : 'bi bi-arrows-expand-vertical me-1'"></i>
				{{ preferencesStore.gridLayout === "scroll" ? "Fit width" : "Scroll" }}
			</button>
		</div>
	`,
};

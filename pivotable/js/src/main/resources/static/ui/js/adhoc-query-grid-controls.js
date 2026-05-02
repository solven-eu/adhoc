import { inject } from "vue";

import AdhocGridFormatModal from "./adhoc-query-grid-format-modal.js";
import AdhocGridExportCsv from "./adhoc-query-grid-export-csv.js";

import { usePreferencesStore } from "./store-preferences.js";

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
		// Provided by AdhocQueryExecutor — the Submit trigger + auto-query state + in-flight signal +
		// "is the in-flight query identical to the displayed one" signal (used to differentiate
		// Refreshing vs Querying labels). Defaults are no-ops so this component still mounts cleanly in
		// test contexts that don't include the executor.
		const submitQuery = inject("submitQuery", () => {});
		const autoQuery = inject("autoQuery", { value: false });
		const isQueryInFlight = inject("isQueryInFlight", { value: false });
		const isSameAsLastQuery = inject("isSameAsLastQuery", { value: false });
		return { preferencesStore, toggleWizardHidden, submitQuery, autoQuery, isQueryInFlight, isSameAsLastQuery };
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
				:class="isQueryInFlight ? 'adhoc-busy' : ''"
				@click="submitQuery"
				:disabled="isQueryInFlight"
				:title="isQueryInFlight ? 'A query is already running' : 'Re-run the current query'"
			>
				<span v-if="isQueryInFlight">
					<span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>
					{{ isSameAsLastQuery ? "Refreshing…" : "Querying…" }}
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
		</div>
	`,
};

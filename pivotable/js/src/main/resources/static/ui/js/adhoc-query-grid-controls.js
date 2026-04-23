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
		return { preferencesStore, toggleWizardHidden };
	},
	template: /* HTML */ `
		<div class="d-flex flex-wrap gap-2 align-items-center mt-2">
			<AdhocGridExportCsv :array="dataArray" />
			<AdhocGridFormatModal :formatOptions="formatOptions" />
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

import { ref, watch, onMounted, reactive, provide, inject, computed } from "vue";

import AdhocCellModal from "./adhoc-query-grid-cell-modal.js";
import AdhocGridFormatModal from "./adhoc-query-grid-format-modal.js";
import AdhocGridExportCsv from "./adhoc-query-grid-export-csv.js";

import { usePreferencesStore } from "./store-preferences.js";

// Formatters
import { SlickGrid, SlickDataView } from "slickgrid";

import gridHelper from "./adhoc-query-grid-helper.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocCellModal,
		AdhocGridFormatModal,
		AdhocGridExportCsv,
	},
	// https://vuejs.org/guide/components/props.html
	props: {
		domId: {
			type: String,
			required: true,
		},
		tabularView: {
			type: Object,
			required: true,
		},
		loading: {
			type: Boolean,
			default: false,
		},
		// Used to remove columns from the grid
		queryModel: {
			type: Object,
			required: false,
		},
		// Used to feed the cellModal with cube details, like measures underlyings
		cube: {
			type: Object,
			required: true,
		},
	},
	setup(props) {
		// https://stackoverflow.com/questions/2402953/javascript-data-grid-for-millions-of-rows
		let dataView;

		let grid;
		const gridMetadata = reactive({});

		const formatOptions = reactive({
			// https://stackoverflow.com/questions/673905/how-can-i-determine-a-users-locale-within-the-browser
			locale: navigator.languages && navigator.languages.length ? navigator.languages[0] : navigator.language,
			// May defaulted to `EUR`
			measureCcy: "",
			// After this number of digits, numbers are simplified with `0`s.
			// TODO This is disabled as it leads to unexpected formatting even with `roundingPriority=morePrecision`
			// e.g. `12.3` is formatted `12.3` instead of `12.30`
			// maximumSignificantDigits: 9,
			// Minumum number of decimals
			minimumFractionDigits: 2,
			// Maximum number of decimals
			maximumFractionDigits: 2,
			// Default is 'auto'
			// roundingPriority: 'morePrecision',
		});

		let gridColumns = [];

		const rendering = ref(false);

		function renderingDone() {
			rendering.value = false;
			props.tabularView.loading.rendering = false;
			if (props.tabularView.timing.rendering_start) {
				props.tabularView.timing.rendering = new Date() - props.tabularView.timing.rendering_start;
				delete props.tabularView.timing.rendering_start;
			} else {
				// another cell already registered renderering as done
			}
		}

		function renderCallback(cellNode, row, dataContext, colDef) {
			// We assume rendering is done as soon as one cell is rendered
			renderingDone();
		}

		const data = reactive({ array: [] });

		const currentSortCol = reactive({ sortId: undefined, sortAsc: true });

		// https://github.com/6pac/SlickGrid/wiki/Providing-data-to-the-grid
		let resyncData = function () {
			const view = props.tabularView.view;

			gridColumns = [];

			// Null view = cleared state (e.g. right after Reset, when queryModel is empty so no
			// query was fired). Render a blank grid with a single placeholder column and zero
			// data rows — SlickGrid misbehaves with `setColumns([])`, so keep at least one column.
			// Bail out before the code below, which dereferences `view.coordinates` and
			// `tabularView.query.groupBy.columns` (both undefined in this state).
			if (!view) {
				gridColumns.push({ id: "empty", name: "", field: "empty", sortable: false });
				data.array = [];
				gridMetadata.nb_rows = 0;
				grid.setColumns(gridColumns);
				dataView.setItems(data.array, "id");
				dataView.refresh();
				return;
			}

			// Do not allow sorting until it is compatible with rowSpans
			const sortable = gridHelper.isSortable();

			// We always show the id column
			// It is especially useful on the grandTotal without measure: the idColumn enables the grid not to be empty
			// If the grid was empty, `renderCallback` would not be called-back
			{
				const column = { id: "id", name: "#row", field: "id", width: 5, sortable: sortable, asyncPostRender: renderCallback };
				gridColumns.push(column);
			}

			data.array = [];

			// from rowIndex to columnIndex to span height
			const metadata = {
				// We leave an example rowSpan but it will reset on first dataset
				0: {
					columns: {
						1: { rowspan: 3 },
					},
				},
			};
			// Delete the example
			delete metadata[0];

			// May be String or Object (decorating a column with calculated coordinates)
			const rawColumnNames = props.tabularView.query.groupBy.columns;
			const columnNames = rawColumnNames.map((rawC) => {
				if (typeof rawC === "object") {
					return rawC.column;
				} else {
					return rawC;
				}
			});

			if (view.coordinates.length === 0) {
				// TODO Why do we show an empty column? Maybe to force having something to render
				const column = { id: "empty", name: "empty", field: "empty", sortable: sortable, asyncPostRender: renderCallback };

				gridColumns.push(column);
				data.array.push({ id: "0", empty: "empty" });
				gridMetadata.nb_rows = 0;

				// TODO How to know when the empty grid is rendered? (This may be slow if previous grid was large)
			} else {
				rendering.value = true;
				props.tabularView.loading.rendering = true;
				props.tabularView.timing.rendering_start = new Date();

				// https://stackoverflow.com/questions/1232040/how-do-i-empty-an-array-in-javascript
				console.log(`Rendering columnNames=${columnNames}`);
				gridColumns.push(...gridHelper.groupByToGridColumns(columnNames, props.queryModel, renderCallback));

				// measureNames may be filled on first row if we requested no measure and received the default measure
				const measureNames = props.tabularView.query.measures;
				console.log(`Rendering measureNames=${measureNames}`);
				// TODO Refresh the columns on `formatOptions` changes, else we need to query to see the format changes
				gridColumns.push(...gridHelper.measuresToGridColumns(measureNames, props.queryModel, renderCallback, formatOptions));

				{
					props.tabularView.loading.sorting = true;
					const sortingStart = new Date();
					try {
						gridHelper.sortRows(columnNames, view.coordinates, view.values);
					} finally {
						props.tabularView.loading.sorting = false;
						props.tabularView.timing.sorting = new Date() - sortingStart;
					}
				}

				props.tabularView.loading.rowSpanning = true;
				const rowSpanningStart = new Date();
				try {
					if (view.coordinates.length >= 1) {
						const rowIndex = 0;
						const coordinatesRow = view.coordinates[rowIndex];
						const measuresRow = view.values[rowIndex];

						gridHelper.sanityCheckFirstRow(columnNames, coordinatesRow, measureNames, measuresRow);
					}

					// https://github.com/vuejs/core/issues/13826
					// RangeError: Maximum call stack size exceeded
					data.array = data.array.concat(gridHelper.toData(columnNames, view.coordinates, measureNames, view.values));

					gridHelper.computeRowSpan(columnNames, metadata, view.coordinates);
				} finally {
					props.tabularView.loading.rowSpanning = false;
					props.tabularView.timing.rowSpanning = new Date() - rowSpanningStart;
				}
			}

			console.debug("rowSpans: ", metadata);

			grid.setColumns(gridColumns);

			gridHelper.updateFooters(grid, columnNames, view.coordinates, view.values);

			dataView.getItemMetadata = (row) => {
				return metadata[row] && metadata[row].attributes ? metadata[row] : (metadata[row] = { attributes: { "data-row": row }, ...metadata[row] });
			};

			// https://github.com/6pac/SlickGrid/wiki/DataView#batching-updates
			dataView.beginUpdate();
			dataView.setItems(data.array);
			dataView.endUpdate();

			gridMetadata.nb_rows = data.array.length;

			// https://github.com/6pac/SlickGrid/issues/1114
			grid.remapAllColumnsRowSpan();

			// https://github.com/6pac/SlickGrid/wiki/Slick.Grid#invalidate
			// since we have a rowspan that spans nearly the entire length to the bottom,
			// we need to invalidate everything so that it recalculate all rowspan cell heights
			grid.invalidate();
		};

		// https://stackoverflow.com/questions/12128680/slickgrid-what-is-a-data-view
		dataView = new SlickDataView({});

		dataView.setItems([]);

		// https://github.com/6pac/SlickGrid/wiki/Grid-Options
		let options = {
			// Do not allow re-ordering until it is compatible with rowSpans
			enableColumnReorder: true,
			enableAutoSizeColumns: true,
			// https://github.com/6pac/SlickGrid/wiki/Auto-Column-Sizing
			// autosizeColsMode: "?"
			//			autoHeight: true,
			fullWidthRows: true,
			// `forceFitColumns` is legacy, and related with `autosizeColsMode`
			forceFitColumns: true,
			// https://github.com/6pac/SlickGrid/blob/master/examples/example10-async-post-render.html		,
			enableAsyncPostRender: true,
			// rowSpan enables showing a single time each value on given column
			enableCellRowSpan: true,
			// rowspan doesn't render well with 'transform', default is 'top'
			// https://github.com/6pac/SlickGrid/blob/master/examples/example-0032-row-span-many-columns.html
			rowTopOffsetRenderType: "top",

			// https://github.com/6pac/SlickGrid/blob/master/examples/example-footer-totals.html
			createFooterRow: true,
			showFooterRow: true,
			footerRowHeight: 28,

			// `rowIndex` column is frozen
			frozenColumn: 1,
		};

		// Initialize with `-1` to have a nice default value
		const clickedCell = ref({ id: "-1" });

		// Use AutoResizer?
		// https://6pac.github.io/SlickGrid/examples/example15-auto-resize.html

		onMounted(() => {
			// SlickGrid requires the DOM to be ready: `onMounted` is needed
			grid = new SlickGrid("#" + props.domId, dataView, gridColumns, options);

			gridHelper.registerHeaderButtons(grid, props.queryModel);

			dataView.refresh();

			gridHelper.registerEventSubscribers(grid, dataView, currentSortCol, clickedCell);

			// Register the watch once the grid is mounted and initialized.
			//
			// NOT `{ deep: true }` on purpose. `view` is only ever swapped by reference from
			// `onView` (a fresh server response) or nulled by the empty-query short-circuit;
			// we never patch it in place. Conversely, `resyncData` itself calls `sortRows`
			// which mutates `view.coordinates` / `view.values` IN-PLACE — with deep tracking,
			// that would re-fire this very watcher, causing the grid to render twice per
			// edit (and logging `Rendering measureNames=` twice). Shallow reference watching
			// fires exactly once per server response — which is what the UX needs.
			watch(
				() => props.tabularView.view,
				(newView, oldView) => {
					console.debug("Detected grid data change", newView, oldView);
					if (!props.tabularView.loading) {
						props.tabularView.loading = {};
					}
					if (!props.tabularView.timing) {
						props.tabularView.timing = {};
					}
					props.tabularView.loading.preparingGrid = true;
					const startPreparingGrid = new Date();
					try {
						resyncData();
					} finally {
						props.tabularView.loading.preparingGrid = false;
						props.tabularView.timing.preparingGrid = new Date() - startPreparingGrid;
					}
				},
			);
		});

		function isLoading() {
			if (!props.tabularView.loading) {
				// Not a single flag initialized the loading property
				return false;
			}

			// BEWARE some properties are date (like latestFetched)
			return Object.values(props.tabularView.loading).some((loadingFlag) => typeof loadingFlag === "boolean" && !!loadingFlag);
		}

		function loadingPercent() {
			if (!isLoading()) {
				return 100;
			}

			if (props.tabularView.loading.sending) {
				return 10;
			}
			if (props.tabularView.loading.executing) {
				return 20;
			}
			if (props.tabularView.loading.downloading) {
				return 75;
			}
			if (props.tabularView.loading.preparingGrid) {
				return 85;
			}
			if (props.tabularView.loading.rendering) {
				return 90;
			}

			console.log("Unclear loading state", props.tabularView.loading);
			return 95;
		}

		function loadingMessage() {
			if (!isLoading()) {
				return "Loaded";
			}

			if (props.tabularView.loading.sending) {
				return "Sending the query";
			}
			if (props.tabularView.loading.executing) {
				// The execution phase may be a single synchronous call, or a polling until state of DONE
				if (props.tabularView.loading.fetching) {
					return "Executing the query (fetching)";
				}
				if (props.tabularView.loading.sleeping) {
					return "Executing the query (sleeping)";
				}
				return "Executing the query (?)";
			}
			if (props.tabularView.loading.downloading) {
				return "Downloading the result";
			}
			if (props.tabularView.loading.preparingGrid) {
				return "Preparing the grid";
			}
			if (props.tabularView.loading.rendering) {
				return "Rendering the grid";
			}

			console.log("Unclear loading state", props.tabularView.loading);
			return "Unclear but not done yet";
		}

		// Human-readable view of the per-stage timings emitted by the executor / grid hooks.
		// The raw `tabularView.timing` object — shape `{ sending, executing, downloading,
		// preparingGrid, sorting, rowSpanning, rendering, … }`, values in milliseconds — used
		// to be rendered as-is under the grid which looked like dev debug output. We now surface
		// it as an ordered list of `<stage>: <ms>ms` pills plus a total, muted because these are
		// operational metrics, not part of the query's functional result.
		const TIMING_ORDER = ["sending", "executing", "downloading", "preparingGrid", "sorting", "rowSpanning", "rendering"];
		const formattedTimings = computed(() => {
			const timing = props.tabularView && props.tabularView.timing;
			if (!timing) return null;

			const entries = [];
			let total = 0;
			// Ordered entries first so the display reads in execution order.
			for (const stage of TIMING_ORDER) {
				const ms = timing[stage];
				if (typeof ms === "number" && Number.isFinite(ms)) {
					entries.push({ stage, ms });
					total += ms;
				}
			}
			// Any unknown keys get appended after in insertion order — keeps forward-compat
			// when the executor starts reporting new stages without needing a UI change.
			for (const stage of Object.keys(timing)) {
				if (TIMING_ORDER.includes(stage)) continue;
				const ms = timing[stage];
				if (typeof ms === "number" && Number.isFinite(ms)) {
					entries.push({ stage, ms });
					total += ms;
				}
			}
			if (entries.length === 0) return null;
			return { entries, total };
		});

		// Shared preferences store — exposes `wizardHidden` for the full-screen-grid toggle.
		// Toggled from a small button rendered next to the Formatting Options / Export buttons
		// at the bottom of the grid area (keeps the toggle close to the other grid-level
		// controls and avoids eating vertical space at the top).
		const preferencesStore = usePreferencesStore();
		const toggleWizardHidden = function () {
			preferencesStore.wizardHidden = !preferencesStore.wizardHidden;
		};

		return {
			rendering,
			gridMetadata,
			clickedCell,
			isLoading,
			loadingPercent,
			loadingMessage,

			formatOptions,
			formattedTimings,

			data,

			preferencesStore,
			toggleWizardHidden,
		};
	},
	template: /* HTML */ `
		<div>
			<div class="spinner-grow" role="status" v-if="loading">
				<span class="visually-hidden">Loading...</span>
			</div>

			<AdhocCellModal :queryModel="queryModel" :clickedCell="clickedCell" :cube="cube" />

			<span style="width:100%;" class="position-relative">
				<div :id="domId" class="vh-75 slickgrid-grid"></div>

				<div class="position-absolute top-50 start-50 translate-middle" style="width:100%;" v-if="isLoading()">
					<div
						class="progress"
						role="progressbar"
						aria-label="Animated striped example"
						:aria-valuenow="loadingPercent()"
						aria-valuemin="0"
						aria-valuemax="100"
						v-if="isLoading()"
					>
						<div class="progress-bar progress-bar-striped progress-bar-animated" :style="'width: ' + loadingPercent() + '%'">
							{{loadingMessage()}}
						</div>
					</div>
				</div>
			</span>
			<div hidden>props.tabularView.loading={{tabularView.loading}}</div>
			<!--
				Per-stage query timings. These are operational metrics (non-functional) — they
				describe how long each phase of the round-trip took, not anything about the data
				itself — hence the muted styling and the explicit "Performance" label so users
				don't confuse them with results. Hidden entirely when no timings are available
				(first render, cached views).
			-->
			<div v-if="formattedTimings" class="small text-muted mt-1" title="Operational metrics — not part of the query result">
				<i class="bi bi-speedometer2 me-1"></i>
				<span class="fw-semibold me-1">Performance:</span>
				<span v-for="(entry, i) in formattedTimings.entries" :key="entry.stage" class="me-2">
					{{entry.stage}}={{entry.ms}}ms<span v-if="i < formattedTimings.entries.length - 1">,</span>
				</span>
				<span class="ms-1">(total: {{formattedTimings.total}}ms)</span>
			</div>
			<!--
				Grid-level controls grouped on a single horizontal strip at the bottom: export
				to CSV, formatting options, and the full-screen-grid toggle (hide the wizard
				to let the grid use the full viewport width). The toggle lives here rather
				than at the top of the grid column so it doesn't push the data down by a row
				that would stay empty most of the time.
			-->
			<div class="d-flex flex-wrap gap-2 align-items-center mt-2">
				<AdhocGridExportCsv :array="data.array" />
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
		</div>
	`,
};

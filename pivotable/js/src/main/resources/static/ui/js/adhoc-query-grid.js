import { ref, computed, watch, onMounted, reactive, provide, inject } from "vue";

import AdhocCellModal from "./adhoc-query-grid-cell-modal.js";
import AdhocGridTimingsBar from "./adhoc-query-grid-timings-bar.js";
import AdhocGridControls from "./adhoc-query-grid-controls.js";
import AdhocMeasureStatsModal from "./adhoc-query-grid-stats-modal.js";

// Formatters
import { SlickGrid, SlickDataView } from "slickgrid";

import gridHelper from "./adhoc-query-grid-helper.js";
import { isLoading as isLoadingHelper, loadingPercent as loadingPercentHelper, loadingMessage as loadingMessageHelper } from "./adhoc-query-grid-loading.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocCellModal,
		AdhocGridTimingsBar,
		AdhocGridControls,
		AdhocMeasureStatsModal,
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

		// Singleton model for the per-measure Statistics modal — provided by `adhoc-query.js`
		// so the grid header buttons (registered in `adhoc-query-grid-helper.js`) can toggle
		// it without prop-drilling. Read here so the local AdhocMeasureStatsModal gets the
		// same reactive object the header buttons mutate.
		const measureStatsModel = inject("measureStatsModel");

		let grid;
		const gridMetadata = reactive({});
		// Two distinct "empty" states with different UX:
		//
		//   - `isEmptyModel` — the queryModel itself has no selected measures and no
		//     selected columns (the user hasn't built a query yet). This is the case
		//     that warrants the wizard-pointer hint ("Use the wizard…"). Derived as a
		//     `computed` so it tracks the queryModel reactively without being mutated
		//     from resyncData.
		//
		//   - `isEmptyView` — the user HAS built a query, but the backend returned zero
		//     rows. The grid still shows its column headers; we don't want the wizard
		//     hint here (it would lie about the state) but we may want a different
		//     "no rows match" message in the future.
		const isEmptyModel = computed(() => {
			const qm = props.queryModel;
			if (!qm) return true;
			const hasSelectedMeasure = Object.values(qm.selectedMeasures || {}).some((v) => v === true);
			const hasSelectedColumn = Object.values(qm.selectedColumns || {}).some((v) => v === true);
			return !hasSelectedMeasure && !hasSelectedColumn;
		});
		const isEmptyView = computed(() => !isEmptyModel.value && !gridMetadata.nb_rows);

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
			if (props.tabularView.timing.rendering_startedAt) {
				props.tabularView.timing.rendering = new Date() - props.tabularView.timing.rendering_startedAt;
				delete props.tabularView.timing.rendering_startedAt;
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
			// Per-measure stats (min / max / sum / count) computed from `view.values`. Set in
			// the non-empty branch below and threaded into both `measuresToGridColumns` (for
			// the heatmap cell formatter) and `updateFooters` (for the min/sum/max summary).
			// Left undefined when the grid is empty so formatters degrade to plain numbers.
			let measureStats;

			// Null view = cleared state (e.g. right after Reset, when queryModel is empty so no
			// query was fired). Render a blank grid with a single placeholder column and zero
			// data rows — SlickGrid misbehaves with `setColumns([])`, so keep at least one column.
			// Bail out before the code below, which dereferences `view.coordinates` and
			// `tabularView.query.groupBy.columns` (both undefined in this state).
			//
			// SlickGrid's frozen-column + rowSpan combo does NOT redraw on `dataView.refresh()`
			// alone — the previous viewport's rendered rows survive. We must explicitly
			// invalidate the row cache and re-render, mirroring the trailing call in the
			// non-empty branch below.
			if (!view) {
				console.log("Rendering empty view (no query)");
				gridColumns.push({ id: "empty", name: "", field: "empty", sortable: false });
				data.array = [];
				gridMetadata.nb_rows = 0;
				grid.setColumns(gridColumns);
				dataView.beginUpdate();
				dataView.setItems(data.array, "id");
				dataView.endUpdate();
				grid.remapAllColumnsRowSpan();
				grid.invalidate();
				grid.render();
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
				props.tabularView.timing.rendering_startedAt = new Date();

				// https://stackoverflow.com/questions/1232040/how-do-i-empty-an-array-in-javascript
				console.log(`Rendering columnNames=${columnNames}`);
				gridColumns.push(...gridHelper.groupByToGridColumns(columnNames, props.queryModel, renderCallback));

				// measureNames may be filled on first row if we requested no measure and received the default measure
				const measureNames = props.tabularView.query.measures;
				console.log(`Rendering measureNames=${measureNames}`);

				// Compute per-measure min/max/sum stats BEFORE building the column definitions
				// so the cell formatter can paint a heatmap background on each numeric cell, and
				// so the footer row can surface min/sum/max without re-scanning the values.
				measureStats = gridHelper.computeMeasureStats(measureNames, view.values);
				// Stash the freshly-computed stats on the shared singleton so the per-measure
				// Statistics modal can be opened from the grid header without re-scanning the
				// view. Resetting the visible measure name on each resync prevents stale
				// numbers from being shown if the user re-opens the modal after a fresh query.
				if (measureStatsModel) {
					measureStatsModel.allStats = measureStats;
				}

				// TODO Refresh the columns on `formatOptions` changes, else we need to query to see the format changes
				gridColumns.push(...gridHelper.measuresToGridColumns(measureNames, props.queryModel, renderCallback, formatOptions, measureStats));

				{
					props.tabularView.loading.sorting = true;
					const sortingStart = new Date();
					props.tabularView.timing.sorting_startedAt = sortingStart;
					try {
						gridHelper.sortRows(columnNames, view.coordinates, view.values);
					} finally {
						props.tabularView.loading.sorting = false;
						props.tabularView.timing.sorting = new Date() - sortingStart;
						delete props.tabularView.timing.sorting_startedAt;
					}
				}

				props.tabularView.loading.rowSpanning = true;
				const rowSpanningStart = new Date();
				props.tabularView.timing.rowSpanning_startedAt = rowSpanningStart;
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
					delete props.tabularView.timing.rowSpanning_startedAt;
				}
			}

			console.debug("rowSpans: ", metadata);

			grid.setColumns(gridColumns);

			gridHelper.updateFooters(grid, columnNames, view.coordinates, view.values, measureStats, formatOptions);

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
					props.tabularView.timing.preparingGrid_startedAt = startPreparingGrid;
					try {
						resyncData();
					} finally {
						props.tabularView.loading.preparingGrid = false;
						props.tabularView.timing.preparingGrid = new Date() - startPreparingGrid;
						delete props.tabularView.timing.preparingGrid_startedAt;
					}
				},
			);
		});

		// Loading-state helpers. All three read from `props.tabularView.loading` and return a
		// display-layer primitive; they live in `adhoc-query-grid-loading.js` so they can be
		// unit-tested without a DOM. Thin instance wrappers are kept here because Vue templates
		// call methods with no args — the wrappers bind `props.tabularView` once.
		const isLoading = () => isLoadingHelper(props.tabularView);
		const loadingPercent = () => loadingPercentHelper(props.tabularView);
		const loadingMessage = () => loadingMessageHelper(props.tabularView);

		return {
			rendering,
			gridMetadata,
			clickedCell,
			isLoading,
			loadingPercent,
			loadingMessage,

			formatOptions,

			data,
			measureStatsModel,
			isEmptyModel,
			isEmptyView,
		};
	},
	template: /* HTML */ `
		<div>
			<div class="spinner-grow" role="status" v-if="loading">
				<span class="visually-hidden">Loading...</span>
			</div>

			<AdhocCellModal :queryModel="queryModel" :clickedCell="clickedCell" :cube="cube" />
			<AdhocMeasureStatsModal :statsModel="measureStatsModel" :formatOptions="formatOptions" />

			<span style="width:100%;" class="position-relative">
				<div :id="domId" class="vh-75 slickgrid-grid"></div>

				<!--
					Empty-state hints. Two variants depending on which kind of "empty" we
					are looking at:
					  - 'isEmptyModel' (no measures + no columns picked yet): point the
					    user to the wizard panel on the left.
					  - 'isEmptyView'  (query was sent but matched zero rows): the typical
					    cause is an over-constrained filter, so point UP to the filter
					    block above the wizard.
				-->
				<div class="position-absolute top-50 start-50 translate-middle text-center text-muted" v-if="!isLoading() && isEmptyModel">
					<i class="bi bi-arrow-left-circle fs-3"></i>
					<div>Use the wizard to pick columns and measures, then Submit to build a query.</div>
				</div>
				<div class="position-absolute top-50 start-50 translate-middle text-center text-muted" v-else-if="!isLoading() && isEmptyView">
					<i class="bi bi-arrow-up-circle fs-3"></i>
					<div>No rows match the current filter.</div>
					<div class="small">Loosen or clear the filter above the wizard to bring rows back.</div>
				</div>

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
			<AdhocGridTimingsBar :tabularView="tabularView" />
			<AdhocGridControls :dataArray="data.array" :formatOptions="formatOptions" />
		</div>
	`,
};

import { ref, watch, onMounted, reactive, provide, inject } from "vue";

import AdhocCellModal from "./adhoc-query-grid-cell-modal.js";
import AdhocGridTimingsBar from "./adhoc-query-grid-timings-bar.js";
import AdhocGridControls from "./adhoc-query-grid-controls.js";

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
			<AdhocGridTimingsBar :tabularView="tabularView" />
			<AdhocGridControls :dataArray="data.array" :formatOptions="formatOptions" />
		</div>
	`,
};

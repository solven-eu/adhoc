import { ref, watch, onMounted, reactive } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import AdhocCellModal from "./adhoc-query-grid-cell-modal.js";
import AdhocGridFormat from "./adhoc-query-grid-format.js";

import { useUserStore } from "./store-user.js";

import { SlickGrid, SlickDataView, Formatters, SlickHeaderButtons } from "slickgrid";

// Ordering of rows
import _ from "lodashEs";

import gridHelper from "./adhoc-query-grid-helper.js";

// BEWARE: Should probably push an event to the Modal component so it open itself
import { Modal } from "bootstrap";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocCellModal,
		AdhocGridFormat,
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
		let data = [];
		const gridMetadata = reactive({});

		const formatOptions = reactive({
			// https://stackoverflow.com/questions/673905/how-can-i-determine-a-users-locale-within-the-browser
			locale: navigator.languages && navigator.languages.length ? navigator.languages[0] : navigator.language,
			// May defaulted to `EUR`
			measureCcy: "",
			// After this number of digits, numbers are simplified with `0`s.
			measureMaxDigits: 9,
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

		// https://github.com/6pac/SlickGrid/wiki/Providing-data-to-the-grid
		let resyncData = function () {
			const view = props.tabularView.view;

			gridColumns = [];

			// Do not allow sorting until it is compatible with rowSpans
			const sortable = gridHelper.isSortable();

			// We always show the id column
			// It is especially useful on the grandTotal without measure: the idColumn enables the grid not to be empty
			// If the grid was empty, `renderCallback` would not be called-back
			{
				const column = { id: "id", name: "#row", field: "id", width: 5, sortable: sortable, asyncPostRender: renderCallback };
				gridColumns.push(column);
			}

			data = [];

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

			const columnNames = props.tabularView.query.groupBy.columns;

			if (view.coordinates.length === 0) {
				// TODO Why do we show an empty column? Maybe to force having something to render
				const column = { id: "empty", name: "empty", field: "empty", sortable: sortable, asyncPostRender: renderCallback };

				gridColumns.push(column);
				data.push({ id: "0", empty: "empty" });
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

					data.push(...gridHelper.toData(columnNames, view.coordinates, measureNames, view.values));

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
			dataView.setItems(data);
			dataView.endUpdate();

			gridMetadata.nb_rows = data.length;

			// https://github.com/6pac/SlickGrid/wiki/Slick.Grid#invalidate
			// since we have a rowspan that spans nearly the entire length to the bottom,
			// we need to invalidate everything so that it recalculate all rowspan cell heights
			grid.invalidate();
			// https://github.com/6pac/SlickGrid/issues/1114
			grid.remapAllColumnsRowSpan();
		};

		// https://stackoverflow.com/questions/12128680/slickgrid-what-is-a-data-view
		dataView = new SlickDataView({});

		dataView.setItems(data);

		// https://github.com/6pac/SlickGrid/wiki/Grid-Options
		let options = {
			// Do not allow re-ordering until it is compatible with rowSpans
			enableColumnReorder: false,
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

			// https://github.com/6pac/SlickGrid/blob/master/examples/example-plugin-headerbuttons.html
			{
				var headerButtonsPlugin = new SlickHeaderButtons();

				headerButtonsPlugin.onCommand.subscribe(function (e, args) {
					var column = args.column;
					var button = args.button;
					var command = args.command;

					if (command == "remove-column") {
						props.queryModel.selectedColumns[column.name] = false;
						props.queryModel.onColumnToggled(column.name);

						// No need to invalidate the grid, as the queryModel change shall trigger a grid/tabularView/data update
						// grid.invalidate();
					} else if (command == "remove-measure") {
						props.queryModel.selectedMeasures[column.name] = false;

						// No need to invalidate the grid, as the queryModel change shall trigger a grid/tabularView/data update
						// grid.invalidate();
					}
				});

				grid.registerPlugin(headerButtonsPlugin);
			}

			// https://stackoverflow.com/questions/11404711/how-can-i-trigger-a-bootstrap-modal-programmatically
			// https://stackoverflow.com/questions/71432924/vuejs-3-and-bootstrap-5-modal-reusable-component-show-programmatically
			// https://getbootstrap.com/docs/5.0/components/modal/#via-javascript
			let cellModal = new Modal(document.getElementById("cellModal"), {});

			// Cell Modal
			function openCellModal(cell) {
				// https://getbootstrap.com/docs/5.0/components/modal/#show
				cellModal.show();

				console.log("Showing modal for cell", cell);
			}

			dataView.refresh();

			// TODO comparer function is never called?
			function comparer(a, b) {
				let x = a[sortcol],
					y = b[sortcol];
				return x == y ? 0 : x > y ? 1 : -1;
			}

			let sortdir = 1;
			let sortcol = "unknown";
			grid.onSort.subscribe(function (e, args) {
				sortdir = args.sortAsc ? 1 : -1;
				sortcol = args.sortCol.field;

				// using native sort with comparer
				// preferred method but can be very slow in IE with huge datasets
				dataView.sort(comparer, args.sortAsc);
			});

			// https://stackoverflow.com/questions/8365139/slickgrid-how-to-get-the-grid-item-on-click-event
			grid.onDblClick.subscribe(function (e, args) {
				var item = dataView.getItem(args.row);

				// Update a reactive: Used to feel the modal content, but not to trigger its opening.
				// It is not used for opening event, else clicking again the same cell would not trigger an event, hence no re-opening of the modal
				clickedCell.value = item;

				openCellModal(clickedCell.value);
			});

			// Register the watch once the grid is mounted and initialized
			watch(
				() => props.tabularView.view,
				(newView, oldView) => {
					console.log("Detected change", newView, oldView);
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
				{ deep: true },
			);
		});

		function isLoading() {
			if (!props.tabularView.loading) {
				// Not a single flag initialized the loading property
				return false;
			}
			return Object.values(props.tabularView.loading).some((loadingFlag) => !!loadingFlag);
		}

		function loadingPercent() {
			if (!isLoading()) {
				return 100;
			}

			if (props.tabularView.loading.sending) {
				return 10;
			}
			if (props.tabularView.loading.downloading) {
				return 35;
			}
			if (props.tabularView.loading.preparingGrid) {
				return 55;
			}
			if (props.tabularView.loading.rendering) {
				return 75;
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

		return {
			rendering,
			gridMetadata,
			clickedCell,
			isLoading,
			loadingPercent,
			loadingMessage,

			formatOptions,
		};
	},
	// :column="column" :type="type" :endpointId="endpointId" :cubeId="cubeId"
	template: /* HTML */ `
        <div>
            <div class="spinner-grow" role="status" v-if="loading">
                <span class="visually-hidden">Loading...</span>
            </div>

            <AdhocCellModal :queryModel="queryModel" :clickedCell="clickedCell" :cube="cube" />

            <div>
                <label>SlickGrid rendering = {{rendering}} ({{gridMetadata}} rows)</label>
            </div>
            <div :id="domId" style="width:100%;" class="vh-75"></div>
            <div
                class="progress"
                role="progressbar"
                aria-label="Animated striped example"
                :aria-valuenow="loadingPercent()"
                aria-valuemin="0"
                aria-valuemax="100"
                v-if="isLoading()"
            >
                <div class="progress-bar progress-bar-striped progress-bar-animated" :style="'width: ' + loadingPercent() + '%'">{{loadingMessage()}}</div>
            </div>
            <div hidden>props.tabularView.loading={{tabularView.loading}}</div>
            <div>props.tabularView.timing={{tabularView.timing}}</div>

            <AdhocGridFormat :formatOptions="formatOptions" />
        </div>
    `,
};

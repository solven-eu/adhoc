import { ref, watch, onMounted, reactive } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocMeasure from "./adhoc-measure.js";

import AdhocCellModal from "./adhoc-grid-cell-modal.js";

import { useUserStore } from "./store-user.js";

import { SlickGrid, SlickDataView, Formatters, SlickHeaderButtons } from "slickgrid";
import Sortable from "sortablejs";

// Ordering of rows
import _ from "lodashEs";

// BEWARE: Should probably push an event to the Modal component so it open itself
import { Modal } from "bootstrap";

// https://github.com/SortableJS/Sortable/issues/1229#issuecomment-521951729
window.Sortable = Sortable;

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocMeasure,
		AdhocCellModal,
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
	},
	setup(props) {
		// https://stackoverflow.com/questions/2402953/javascript-data-grid-for-millions-of-rows
		let dataView;

		let grid;
		let data = [];
		const gridMetadata = reactive({});

		let gridColumns = [];

		// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Intl/NumberFormat
		const numberFormat = new Intl.NumberFormat({ maximumSignificantDigits: 3 });

		// https://github.com/6pac/SlickGrid/blob/master/src/slick.formatters.ts
		function percentFormatter(row, cell, value, columnDef, dataContext) {
			var rtn = {};

			rtn.text = numberFormat.format(value * 100) + "%";
			rtn.toolTip = value;

			return rtn;
		}

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
			const sortable = false;

			// We always show the id column
			// It is especially useful on the grandTotal without measure: the idColumn enables the grid not to be empty
			// If the grid was empty, `renderCallback` would not be called-back
			{
				const column = { id: "id", name: "#row", field: "id", width: 5, sortable: sortable, asyncPostRender: renderCallback };
				gridColumns.push(column);
			}

			data = [];

			// from rowIndex to columnIndex to span height
			// We leave an example rowSpan but it will reset on first dataset
			const metadata = {
				0: {
					columns: {
						1: { rowspan: 3 },
					},
				},
			};
			delete metadata[0];

			if (!view.coordinates) {
				const column = { id: "empty", name: "empty", field: "empty", sortable: sortable, asyncPostRender: renderCallback };

				gridColumns.push(column);
				data.push({ id: "0", empty: "empty" });
				gridMetadata.nb_rows = 0;

				// TODO How to know when the empty grid is rendered? (This may be slow if previous grid was large)
			} else {
				rendering.value = true;
				props.tabularView.loading.rendering = true;
				props.tabularView.timing.rendering_start = new Date();

				// https://stackoverflow.com/questions/9840548/how-to-put-html-into-slickgrid-cell
				//				function popoverFormatter(row, cell, value, columnDef, dataContext) {
				//					return '<span class="h-100 w-100" style="width=100%;" data-bs-toggle="popover" title="someTitle" data-bs-content="popoverContent">' + value + '</span>';
				//				}

				// https://stackoverflow.com/questions/1232040/how-do-i-empty-an-array-in-javascript
				const columnNames = props.tabularView.query.groupBy.columns;
				console.log(`Rendering columnNames=${columnNames}`);
				for (let columnName of columnNames) {
					const column = {
						id: columnName,
						name: columnName,
						field: columnName,
						sortable: sortable,
						asyncPostRender: renderCallback,
						// formatter: popoverFormatter,
					};

					if (props.queryModel) {
						// queryModel is available: show a button to edit the queryModel from the grid
						column.header = {
							buttons: [
								{
									command: "remove-column",
									tooltip: "Remove this groupBy",
									cssClass: "bi bi-x-circle",
									itemVisibilityOverride: function (args) {
										// for example don't show the header button on column "E"
										return args.column.name !== "E";
									},
									itemUsabilityOverride: function (args) {
										// for example the button usable everywhere except on last column "J"
										return args.column.name !== "J";
									},
									action: function (e, args) {
										// you can use the "action" callback and/or subscribe to the "onCallback" event, they both have the same arguments
										// do something
										console.log("Requested removal of groupBy=" + args.column.name);
									},
								},
							],
						};
					}

					gridColumns.push(column);
				}

				// measureNames may be filled on first row if we requested no measure and received the default measure
				const measureNames = props.tabularView.query.measures;
				console.log(`Rendering measureNames=${measureNames}`);
				for (let measureName of measureNames) {
					const column = { id: measureName, name: measureName, field: measureName, sortable: sortable, asyncPostRender: renderCallback };

					if (measureName.indexOf("%") >= 0) {
						column["formatter"] = percentFormatter;
					}

					if (props.queryModel) {
						// queryModel is available: show a button to edit the queryModel from the grid
						column.header = {
							buttons: [
								{
									command: "remove-measure",
									tooltip: "Remove this measure",
									cssClass: "bi bi-x-circle",
									itemVisibilityOverride: function (args) {
										// for example don't show the header button on column "E"
										return args.column.name !== "E";
									},
									itemUsabilityOverride: function (args) {
										// for example the button usable everywhere except on last column "J"
										return args.column.name !== "J";
									},
									action: function (e, args) {
										// you can use the "action" callback and/or subscribe to the "onCallback" event, they both have the same arguments
										// do something
										console.log("Requested removal of measure=" + args.column.name);
									},
								},
							],
						};
					}

					gridColumns.push(column);
				}

				props.tabularView.loading.sorting = true;
				const sortingStart = new Date();
				try {
					// https://stackoverflow.com/questions/48701488/how-to-order-array-by-another-array-ids-lodash-javascript
					const index = _.map(view.coordinates, (x, i) => [view.coordinates[i], view.values[i]]);

					const sortingFunctions = [];
					const sortingOrders = [];
					for (let column of columnNames) {
						sortingFunctions.push(function (item) {
							return item[0][column];
						});
						// or `desc`
						sortingOrders.push("asc");
					}
					const indexSorted = _.orderBy(index, sortingFunctions, sortingOrders);

					for (let i = 0; i < view.coordinates.length; i++) {
						view.coordinates[i] = indexSorted[i][0];
						view.values[i] = indexSorted[i][1];
					}
				} finally {
					props.tabularView.loading.sorting = false;
					props.tabularView.timing.sorting = new Date() - sortingStart;
				}

				// This is used for rowSpan evaluation
				let previousCoordinates = undefined;

				props.tabularView.loading.rowSpanning = true;
				const rowSpanningStart = new Date();
				try {
					const runningRowSpans = {};

					// https://github.com/6pac/SlickGrid/issues/1114
					function updateRowSpan(rowIndex, coordinatesRow, lastRow) {
						if (previousCoordinates) {
							for (const column of columnNames) {
								if (!lastRow && coordinatesRow[column] == previousCoordinates[column]) {
									// In a rowSpan
									if (!runningRowSpans[column]?.isRunning) {
										// Let's start running
										runningRowSpans[column] = {};
										runningRowSpans[column].isRunning = true;
										runningRowSpans[column].startRowIndex = rowIndex - 1;
									}
								} else {
									// Not in a rowSpan
									if (runningRowSpans[column]?.isRunning) {
										// Let's stop running
										const rowSpan = rowIndex - runningRowSpans[column].startRowIndex;

										const columnIndex = columnNames.indexOf(column);

										const rowIndexStart = rowIndex - rowSpan;
										if (!metadata[rowIndexStart]) {
											metadata[rowIndexStart] = { columns: {} };
										}

										// `1+` due to `id` column which is always enforced
										metadata[rowIndexStart].columns[1 + columnIndex] = { rowspan: rowSpan };

										console.debug(`rowSpan for ${column}=${previousCoordinates[column]} from rowIndex=${rowIndexStart} with rowSpan=${rowSpan}`);

										delete runningRowSpans[column];
									}
								}
							}
						}
					}

					// https://github.com/6pac/SlickGrid/blob/master/examples/example-grouping-esm.html
					for (let rowIndex = 0; rowIndex < view.coordinates.length; rowIndex++) {
						const coordinatesRow = view.coordinates[rowIndex];
						const measuresRow = view.values[rowIndex];

						if (rowIndex == 0) {
							// https://stackoverflow.com/questions/29951293/using-lodash-to-compare-jagged-arrays-items-existence-without-order
							if (!_.isEqual(_.sortBy(columnNames), _.sortBy(Object.keys(coordinatesRow)))) {
								throw new Error(`Inconsistent columnNames: ${columnNames} vs ${Object.keys(coordinatesRow)}`);
							}
							if (measureNames.length == 0) {
								// Happens when not selected any measure, but receiving values for the default measure
								// TODO Is this still a legit behavior, since we return slices without columns on such  case?
								for (let measureName of Object.keys(measuresRow)) {
									const column = { id: measureName, name: measureName, field: measureName, sortable: true, asyncPostRender: renderCallback };

									if (measureName.indexOf("%") >= 0) {
										column["formatter"] = percentFormatter;
									}

									measureNames.push(measureName);
									gridColumns.push(column);
								}
							} else if (!_.isEqual(_.sortBy(measureNames), _.sortBy(Object.keys(measuresRow)))) {
								// throw new Error(`Inconsistent measureNames: ${measureNames} vs ${Object.keys(measuresRow)}`);

								// This typically happens when not requesting a single measure, and receiving the default measure
								console.log(`Inconsistent measureNames: ${measureNames} vs ${Object.keys(measuresRow)}`);
							}
						}

						let d = {};

						d["id"] = rowIndex;

						for (const property of columnNames) {
							d[property] = coordinatesRow[property];
						}
						for (const property of measureNames) {
							d[property] = measuresRow[property];
						}

						updateRowSpan(rowIndex, coordinatesRow, false);

						// console.log(d);

						data.push(d);
						previousCoordinates = coordinatesRow;
					}

					// Purge rowSpan after the lastRow
					updateRowSpan(view.coordinates.length, null, true);
				} finally {
					props.tabularView.loading.rowSpanning = false;
					props.tabularView.timing.rowSpanning = new Date() - rowSpanningStart;
				}
			}

			console.debug("rowSpans: ", metadata);

			grid.setColumns(gridColumns);

			// Update footer row
			{
				const columnToDistinctCount = {};
				console.log("columns", grid.getColumns());
				for (let column of grid.getColumns()) {
					const columnName = column.name;
					if ("id" === column.id) {
						// rowIndex column has `distinctCount==length`
						columnToDistinctCount[columnName] = view.coordinates.length;
					} else {
						const values = [];

						for (let rowIndex = 0; rowIndex < view.coordinates.length; rowIndex++) {
							values.push(view.coordinates[rowIndex][columnName]);
						}

						// https://stackoverflow.com/questions/21661686/fastest-way-to-get-count-of-unique-elements-in-javascript-array
						columnToDistinctCount[columnName] = new Set(values).size;
					}

					// https://github.com/6pac/SlickGrid/blob/master/examples/example-footer-totals.html
					var columnElement = grid.getFooterRowColumn(column.id);
					columnElement.textContent = `#: ${columnToDistinctCount[columnName]}`;
				}
			}

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

		watch(
			() => props.tabularView.view,
			() => {
				console.log("Detected change");
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

		// https://stackoverflow.com/questions/12128680/slickgrid-what-is-a-data-view
		dataView = new SlickDataView({});

		dataView.setItems(data);

		// Cell Modal

		// Initialize with `-1` to have a nice default value
		const clickedCell = ref({ id: "-1" });
		{
			function openCellModal(cell) {
				// https://stackoverflow.com/questions/11404711/how-can-i-trigger-a-bootstrap-modal-programmatically
				// https://stackoverflow.com/questions/71432924/vuejs-3-and-bootstrap-5-modal-reusable-component-show-programmatically
				// https://getbootstrap.com/docs/5.0/components/modal/#via-javascript
				let cellModal = new Modal(document.getElementById("cellModal"), {});
				// https://getbootstrap.com/docs/5.0/components/modal/#show
				cellModal.show();

				console.log("Showing modal for cell", cell);
			}

			watch(
				() => clickedCell,
				() => {
					// TODO Make it easy to filter by clicking a cell
					openCellModal(clickedCell.value);
				},
				{ deep: true },
			);
		}

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

				// Update a reactive for event propagation in Vue
				clickedCell.value = item;
			});
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

		return { rendering, gridMetadata, clickedCell, isLoading, loadingPercent, loadingMessage };
	},
	// :column="column" :type="type" :endpointId="endpointId" :cubeId="cubeId"
	template: /* HTML */ `
        <div>
            <div class="spinner-grow" role="status" v-if="loading">
                <span class="visually-hidden">Loading...</span>
            </div>

            <AdhocCellModal :queryModel="queryModel" :clickedCell="clickedCell" />

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
            <div>clickedCell={{clickedCell}}</div>
            <div hidden>props.tabularView.loading={{tabularView.loading}}</div>
            <div>props.tabularView.timing={{tabularView.timing}}</div>
        </div>
    `,
};

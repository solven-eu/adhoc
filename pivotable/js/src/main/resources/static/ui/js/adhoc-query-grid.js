import { ref, watch, onMounted, reactive } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocMeasure from "./adhoc-measure.js";

import { useUserStore } from "./store-user.js";

import { SlickGrid, SlickDataView, Formatters } from "slickgrid";
import Sortable from "sortablejs";

import _ from "lodashEs";

// https://github.com/SortableJS/Sortable/issues/1229#issuecomment-521951729
window.Sortable = Sortable;

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocMeasure,
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

		function renderCallback(cellNode, row, dataContext, colDef) {
			rendering.value = false;
		}

		// from rowIndex to columnIndex to span height
		const metadata = {
		  0: {
		    columns: {
		      1: { rowspan: 3 }
		    }
		  },
		};

		// https://github.com/6pac/SlickGrid/wiki/Providing-data-to-the-grid
		let resyncData = function () {
			const view = props.tabularView.view;
			
			gridColumns=[];
			
			data = [];

			rendering.value = true;
			
			// https://stackoverflow.com/questions/684575/how-to-quickly-clear-a-javascript-object
			for (const rowIndex of Object.getOwnPropertyNames(metadata)) {
			  delete metadata[rowIndex];
			}
			
			// Do not allow sorting until it is compatible with rowSpans
			const sortable = false;

			if (!view.coordinates) {
				const column = { id: "empty", name: "empty", field: "empty", sortable: sortable, asyncPostRender: renderCallback };

				gridColumns.push(column);
				data.push({ id: "0", empty: "empty" });
				gridMetadata.nb_rows = 0;
			} else {
				// https://stackoverflow.com/questions/1232040/how-do-i-empty-an-array-in-javascript
				const columnNames = props.tabularView.query.groupBy.columns;
				console.log(`Rendering columnNames=${columnNames}`);
				for (let columnName of columnNames) {
					const column = { id: columnName, name: columnName, field: columnName, sortable: sortable, asyncPostRender: renderCallback };
					gridColumns.push(column);
				}

				// measureNames may be filled on first row if we requested no measure and received the default measure
				const measureNames = props.tabularView.query.measureRefs;
				console.log(`Rendering measureNames=${measureNames}`);
				for (let measureName of measureNames) {
					const column = { id: measureName, name: measureName, field: measureName, sortable: sortable, asyncPostRender: renderCallback };

					if (measureName.indexOf("%") >= 0) {
						column["formatter"] = percentFormatter;
					}

					gridColumns.push(column);
				}
				
				{
					// https://stackoverflow.com/questions/48701488/how-to-order-array-by-another-array-ids-lodash-javascript
					const index = _.map(view.coordinates, (x, i) => [view.coordinates[i], view.values[i]]);
//					const sorted = _.sortBy(index, coordinateToMeasure => { 
//						return coordinateToMeasure[0][0];
//					});

					const sortingFunctions = [];
					const sortingOrders = [];
					for (let column of columnNames) {
						sortingFunctions.push(function (item) { return item[0][column]; });
						// or `desc`
						sortingOrders.push("asc");
					}
					const indexSorted = _.orderBy(index, sortingFunctions, sortingOrders);
					
					for (let i = 0; i < view.coordinates.length; i++) {
						view.coordinates[i] = indexSorted[i][0];
						view.values[i] = indexSorted[i][1];
					}
				}
				
				// This is used for rowSpan evaluation
				let previousCoordinates = undefined;
				
				const runningRowSpans = {};
				
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
										metadata[rowIndexStart] = {columns: {}};	
									} 
									metadata[rowIndexStart].columns[columnIndex] = { rowspan: rowSpan };
									
									console.debug(`rowSpan for ${column}=${previousCoordinates[column]} from rowIndex=${rowIndexStart} with rowSpan=${rowSpan}`);
									
									delete runningRowSpans[column];
								}
							}
						}
					}
				};
				
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
				updateRowSpan(view.coordinates.length, null,true);
			}
			
			console.log("rowSpans: ", metadata);

			grid.setColumns(gridColumns);
			

			// Option #1
			dataView.getItemMetadata = (row) => {
			  return metadata[row] && metadata.attributes
			    ? metadata[row]
			    : (metadata[row] = {attributes: {'data-row': row}, ...metadata[row]});
			};

			// Option #2
			// const dataView = new Slick.Data.DataView({
			//   globalItemMetadataProvider: {
			//     getRowMetadata: (item, row) => {
			//       return metadata[row];
			//     }
			//   }
			// });

			// https://github.com/6pac/SlickGrid/wiki/DataView#batching-updates
			dataView.beginUpdate();
			dataView.setItems(data);
			dataView.endUpdate();
			
			gridMetadata.nb_rows = data.length;
			
			// https://github.com/6pac/SlickGrid/wiki/Slick.Grid#invalidate
			// since we have a rowspan that spans nearly the entire length to the bottom,
			// we need to invalidate everything so that it recalculate all rowspan cell heights
			grid.invalidate();
		};

		watch(
			() => props.tabularView,
			() => {
				resyncData();
			},
			{ deep: true },
		);
		
		// https://stackoverflow.com/questions/12128680/slickgrid-what-is-a-data-view
		dataView = new SlickDataView({});

		dataView.setItems(data);

		// https://github.com/6pac/SlickGrid/wiki/Grid-Options
		let options = {
			// Do not allow re-ordering until it is compatible with rowSpans
			enableColumnReorder: false,
			enableAutoSizeColumns: true,
			//			autoHeight: true,
			fullWidthRows: true,
			forceFitColumns: true,
			// https://github.com/6pac/SlickGrid/blob/master/examples/example10-async-post-render.html		,
			enableAsyncPostRender: true,
			// rowSpan enables showing a single time each value on given column
			enableCellRowSpan: true,
			// rowspan doesn't render well with 'transform', default is 'top'
			// https://github.com/6pac/SlickGrid/blob/master/examples/example-0032-row-span-many-columns.html
			rowTopOffsetRenderType: 'top' 
		};

		// Use AutoResizer?
		// https://6pac.github.io/SlickGrid/examples/example15-auto-resize.html

		onMounted(() => {
			// SlickGrid requires the DOM to be ready: `onMounted` is needed
			grid = new SlickGrid("#" + props.domId, dataView, gridColumns, options);
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
		});

		return { rendering, gridMetadata };
	},
	template: /* HTML */ `
		<div>
			<div class="spinner-grow" role="status" v-if="loading">
			  <span class="visually-hidden">Loading...</span>
			</div>
			
			<!--div v-for="(row, index) in tabularView.grid?.coordinates">
				{{row}} -> {{tabularView.grid.values[index]}}
			</div-->
			
			<div>
			  <div style="width:100%;">
			    <label>SlickGrid</label>
			  </div>
			  <div :id="domId" style="width:100%;height:500px;"></div>
			</div>
			  rendering = {{rendering}} ({{gridMetadata}} rows)
        </div>
    `,
};

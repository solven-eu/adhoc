import { ref, inject } from "vue";

// Ordering of rows
import _ from "lodashEs";

import Sortable from "sortablejs";

// Formatters
import { SlickHeaderButtons } from "slickgrid";

// BEWARE: Should probably push an event to the Modal component so it open itself
import { Modal } from "bootstrap";

// https://github.com/SortableJS/Sortable/issues/1229#issuecomment-521951729
window.Sortable = Sortable;

// https://stackoverflow.com/questions/59605033/how-to-self-reference-nodejs-module
const isSortable = function () {
	// Do not allow sorting until it is compatible with rowSpans
	return true;
};

const formatters = function (formatOptions) {
	if (!formatOptions) {
		formatOptions = {};
	}

	// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Intl/NumberFormat
	const numberFormatOptions = {};
	numberFormatOptions.maximumSignificantDigits = formatOptions.maximumSignificantDigits;
	numberFormatOptions.minimumFractionDigits = formatOptions.minimumFractionDigits;
	numberFormatOptions.maximumFractionDigits = formatOptions.maximumFractionDigits;

	// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Intl/NumberFormat/NumberFormat
	// has a specific section about `Specifying significant and fractional digits at the same time`
	// `roundingPriority` seems necessary, else `auto` may produce unexpected formatted numbers
	numberFormatOptions.roundingPriority = formatOptions.roundingPriority;

	if (formatOptions.measureCcy) {
		numberFormatOptions.style = "currency";
		numberFormatOptions.currency = formatOptions.measureCcy;
	}
	let numberFormat;
	try {
		numberFormat = new Intl.NumberFormat(formatOptions.locale, numberFormatOptions);
	} catch (e) {
		// May happen on `minimumFractionDigits>maximumFractionDigits`
		console.warn("Invalid formatting options", numberFormatOptions, e);
		numberFormat = new Intl.NumberFormat(formatOptions.locale, {});
	}

	function measureFormatter(row, cell, value, columnDef, dataContext) {
		var rtn = {};

		// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Data_structures
		if (typeof value === "number") {
			rtn.text = numberFormat.format(value);
			// toolTip show raw value
			rtn.toolTip = value;
		} else if (typeof value === "object") {
			rtn.text = JSON.stringify(value);
			// toolTip also shows the JSON
			rtn.toolTip = rtn.text;
		} else {
			// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/typeof
			// string, boolean, bigint, etc
			rtn.text = value;
			// toolTip also show the value
			rtn.toolTip = value;
		}

		return rtn;
	}

	const percentFormatOptions = {};
	percentFormatOptions.maximumSignificantDigits = formatOptions.maximumSignificantDigits;
	// TODO Should we have a different fraction digit for %?
	// percentFormatOptions.maximumFractionDigits = formatOptions.maximumFractionDigits;
	percentFormatOptions.style = "percent";
	const percentFormat = new Intl.NumberFormat(formatOptions.locale, percentFormatOptions);

	// https://github.com/6pac/SlickGrid/blob/master/src/slick.formatters.ts
	function percentFormatter(row, cell, value, columnDef, dataContext) {
		var rtn = {};

		rtn.text = percentFormat.format(value);
		rtn.toolTip = value;

		return rtn;
	}

	return { measureFormatter: measureFormatter, percentFormatter: percentFormatter };
};

// https://github.com/6pac/SlickGrid/issues/1114
const updateRowSpanInner = function (runningRowSpans, columnNames, metadata, rowIndex, coordinatesRow, previousCoordinates, lastRow) {
	if (!previousCoordinates) {
		return;
	}
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
};

export default {
	isSortable,
	formatters,

	sortRows: function (columnNames, coordinates, values) {
		if (coordinates.length != values.length) {
			throw new Error("Incomptable length: coordinates.length=" + coordinates.length + " values.length=" + values.length);
		}

		// https://stackoverflow.com/questions/48701488/how-to-order-array-by-another-array-ids-lodash-javascript
		const index = _.map(coordinates, (x, i) => [coordinates[i], values[i]]);

		const sortingFunctions = [];
		const sortingOrders = [];
		for (let column of columnNames) {
			sortingFunctions.push(function (item) {
				const valueToCompare = item[0][column];

				if (valueToCompare === "*") {
					// TODO Dirty trick to help `*` (grandTotal calculatedCoordinate) as first coordinate
					return "";
				}

				return valueToCompare;
			});
			// or `desc`
			sortingOrders.push("asc");
		}
		const indexSorted = _.orderBy(index, sortingFunctions, sortingOrders);

		for (let i = 0; i < coordinates.length; i++) {
			coordinates[i] = indexSorted[i][0];
			values[i] = indexSorted[i][1];
		}
	},

	sanityCheckFirstRow: function (columnNames, coordinatesRow, measureNames, measuresRow) {
		// https://stackoverflow.com/questions/29951293/using-lodash-to-compare-jagged-arrays-items-existence-without-order
		if (!_.isEqual(_.sortBy(columnNames), _.sortBy(Object.keys(coordinatesRow)))) {
			throw new Error(`Inconsistent columnNames: ${columnNames} vs ${Object.keys(coordinatesRow)}`);
		}
		if (!_.isEqual(_.sortBy(measureNames), _.sortBy(Object.keys(measuresRow)))) {
			// throw new Error(`Inconsistent measureNames: ${measureNames} vs ${Object.keys(measuresRow)}`);

			// This typically happens when not requesting a single measure, and receiving the default measure
			// This typically happens when the first row missed some measures (e.g. due to `value==null`)
			console.debug(`Inconsistent measureNames: ${measureNames} vs ${Object.keys(measuresRow)}`);
		}
	},

	groupByToGridColumns: function (columnNames, queryModel, renderCallback) {
		const gridColumns = [];

		for (let columnName of columnNames) {
			const column = {
				id: columnName,
				name: columnName,
				field: columnName,
				sortable: isSortable(),
				asyncPostRender: renderCallback,
				// formatter: popoverFormatter,
			};

			if (queryModel) {
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
						{
							command: "filter-column",
							tooltip: "Filter over c=" + columnName,
							cssClass: "bi bi-filter-circle",
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
								console.log("Open Filter modal for c=" + args.column.name);
							},
						},
					],
				};
			}

			gridColumns.push(column);
		}

		return gridColumns;
	},

	measuresToGridColumns: function (measureNames, queryModel, renderCallback, formatOptions) {
		const measureFormatters = formatters(formatOptions);

		const gridColumns = [];

		for (let measureName of measureNames) {
			const column = {
				id: measureName,
				name: measureName,
				field: measureName,
				sortable: isSortable(),
				asyncPostRender: renderCallback,
				// Align measures to the right to make it easier to detect large number
				// `font-monospace` is useful to have numbers properly aligned through the column
				cssClass: "text-end font-monospace",
			};

			// Override the style from `.slick-header-column`
			column.headerCssClass = "text-end justify-content-end";

			if (measureName.indexOf("%") >= 0) {
				column["formatter"] = measureFormatters.percentFormatter;
			} else {
				column["formatter"] = measureFormatters.measureFormatter;
			}

			if (queryModel) {
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
						{
							command: "info-measure",
							tooltip: "DAG about m=" + measureName,
							cssClass: "bi bi-question-circle",
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
								console.log("Requested DAG of measure=" + args.column.name);
							},
						},
					],
				};
			}

			gridColumns.push(column);
		}

		return gridColumns;
	},

	computeRowSpan: function (columnNames, metadata, coordinates) {
		const runningRowSpans = {};

		// This is used for rowSpan evaluation
		let previousCoordinates = undefined;

		// https://github.com/6pac/SlickGrid/blob/master/examples/example-grouping-esm.html
		for (let rowIndex = 0; rowIndex < coordinates.length; rowIndex++) {
			const coordinatesRow = coordinates[rowIndex];

			updateRowSpanInner(runningRowSpans, columnNames, metadata, rowIndex, coordinatesRow, previousCoordinates, false);

			previousCoordinates = coordinatesRow;
		}

		// Purge rowSpan after the lastRow
		updateRowSpanInner(runningRowSpans, columnNames, metadata, coordinates.length, null, previousCoordinates, true);
	},

	toData: function (columnNames, coordinates, measureNames, values) {
		const data = [];

		// https://github.com/6pac/SlickGrid/blob/master/examples/example-grouping-esm.html
		for (let rowIndex = 0; rowIndex < coordinates.length; rowIndex++) {
			const coordinatesRow = coordinates[rowIndex];
			const measuresRow = values[rowIndex];

			let d = {};

			d["id"] = rowIndex;

			for (const property of columnNames) {
				d[property] = coordinatesRow[property];
			}
			for (const property of measureNames) {
				d[property] = measuresRow[property];
			}

			// console.log(d);

			data.push(d);
		}

		return data;
	},

	updateFooters: function (grid, columnNames, coordinates, values) {
		// Update footer row
		const columnToDistinctCount = {};

		for (let column of grid.getColumns()) {
			const columnName = column.name;

			var footerText = null;
			if ("id" === column.id) {
				// rowIndex column has `distinctCount==length`
				columnToDistinctCount[columnName] = coordinates.length;

				footerText = `#: ${columnToDistinctCount[columnName]}`;
			} else if (columnNames.includes(columnName)) {
				const values = [];

				for (let rowIndex = 0; rowIndex < coordinates.length; rowIndex++) {
					values.push(coordinates[rowIndex][columnName]);
				}

				// https://stackoverflow.com/questions/21661686/fastest-way-to-get-count-of-unique-elements-in-javascript-array
				columnToDistinctCount[columnName] = new Set(values).size;

				footerText = `#: ${columnToDistinctCount[columnName]}`;
			} else {
				var sum = 0;
				var min = null;
				var max = null;

				for (let rowIndex = 0; rowIndex < values.length; rowIndex++) {
					if (typeof values[rowIndex][columnName] === "number") {
						const asNumber = values[rowIndex][columnName];

						sum += asNumber;
						if (min === null) {
							min = asNumber;
						} else {
							min = Math.min(min, asNumber);
						}
						if (max === null) {
							max = asNumber;
						} else {
							max = Math.min(max, asNumber);
						}
					}
				}

				// TODO Need to properly format else it is not readable
				footerText = `sum=${sum} min=${min} max=${max}`;
				footerText = "";
			}

			if (footerText) {
				// https://github.com/6pac/SlickGrid/blob/master/examples/example-footer-totals.html
				var columnElement = grid.getFooterRowColumn(column.id);
				columnElement.textContent = footerText;
			}
		}
	},
	
	registerHeaderButtons(grid, queryModel) {
		// https://github.com/6pac/SlickGrid/blob/master/examples/example-plugin-headerbuttons.html
		var headerButtonsPlugin = new SlickHeaderButtons();

		const ids = inject("ids");

		// a ref to create a single Modal object
		const measuresDagModal = new Modal(document.getElementById("measureDag"), {});
		const measuresDagModel = inject("measuresDagModel");

		const columnFilterModal = new Modal(document.getElementById("columnFilterModal"), {});
		const columnFilterModel = inject("columnFilterModel");

		headerButtonsPlugin.onCommand.subscribe(function (e, args) {
			var column = args.column;
			var button = args.button;
			var command = args.command;
			

			if (command == "remove-column") {
				queryModel.selectedColumns[column.name] = false;
				queryModel.onColumnToggled(column.name);

				// No need to invalidate the grid, as the queryModel change shall trigger a grid/tabularView/data update
				// grid.invalidate();
			} else if (command == "filter-column") {
				columnFilterModel.column = column.name;
				columnFilterModal.show();
			} else if (command == "remove-measure") {
				queryModel.selectedMeasures[column.name] = false;

				// No need to invalidate the grid, as the queryModel change shall trigger a grid/tabularView/data update
				// grid.invalidate();
			} else if (command == "info-measure") {
				console.log("Info measure", column.name);

				measuresDagModel.main = column.name;
				measuresDagModal.show();
			}
		});

		grid.registerPlugin(headerButtonsPlugin);
	},
	
	registerEventSubscribers(grid, dataView, currentSortCol, clickedCell) {
		// https://github.com/6pac/SlickGrid/wiki/DataView#sorting
		{
			// TODO Refactor this with `gridHelper.sortRows`
			const comparer = function (left, right) {
				const x = left[currentSortCol.sortId];
				const y = right[currentSortCol.sortId];

				const xIsNullOrUndefined = x === null || typeof x === "undefined";
				const yIsNullOrUndefined = y === null || typeof y === "undefined";

				// nullOrUndefined are lower than notNullOrUndefined
				if (xIsNullOrUndefined && yIsNullOrUndefined) {
					return 0;
				} else if (xIsNullOrUndefined) {
					return -1;
				} else if (yIsNullOrUndefined) {
					return 1;
				}

				// `*` represents the grandTotal calculatedMember
				// BEWARE it may lead to confusion if there is an actual `*` coordinate
				const xIsStar = x === "*";
				const yIsStar = y === "*";

				if (xIsStar && yIsStar) {
					return 0;
				} else if (xIsStar) {
					return -1;
				} else if (yIsStar) {
					return 1;
				}

				return x === y ? 0 : x > y ? 1 : -1;
			};

			grid.onSort.subscribe(function (e, args) {
				currentSortCol.sortId = args.sortCol.field;
				currentSortCol.sortAsc = args.sortAsc;

				// using native sort with comparer
				// preferred method but can be very slow in IE with huge datasets
				dataView.sort(comparer, args.sortAsc);

				// Drop the rowSpans until we know how to compute them properly given sortOrders
				// BEWARE It is ugly, but it shows correct figures.
				const metadata = {};
				dataView.getItemMetadata = (row) => {
					return metadata[row] && metadata[row].attributes ? metadata[row] : (metadata[row] = { attributes: { "data-row": row }, ...metadata[row] });
				};

				// https://github.com/6pac/SlickGrid/issues/1114
				grid.remapAllColumnsRowSpan();

				grid.invalidateAllRows();
				grid.render();
			});
		}

		{
			grid.onColumnsReordered.subscribe(function (e, args) {
				console.log("reOrdered columns:", grid.getColumns());

				// Drop the rowSpans until we know how to compute them properly given reorderedColumns
				// BEWARE It is ugly, but it shows correct figures.
				const metadata = {};
				dataView.getItemMetadata = (row) => {
					return metadata[row] && metadata[row].attributes ? metadata[row] : (metadata[row] = { attributes: { "data-row": row }, ...metadata[row] });
				};

				// https://github.com/6pac/SlickGrid/issues/1114
				grid.remapAllColumnsRowSpan();

				grid.invalidateAllRows();
				grid.render();
			});
		}

		{
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
			
			// https://stackoverflow.com/questions/8365139/slickgrid-how-to-get-the-grid-item-on-click-event
			grid.onDblClick.subscribe(function (e, args) {
				var item = dataView.getItem(args.row);
	
				// Update a reactive: Used to feel the modal content, but not to trigger its opening.
				// It is not used for opening event, else clicking again the same cell would not trigger an event, hence no re-opening of the modal
				clickedCell.value = item;
	
				openCellModal(clickedCell.value);
			});
		}

		grid.onHeaderClick.subscribe(function (e, args) {
			const column = args.column.id;
			console.log("Header clicked", column);
		});
		
		// https://stackoverflow.com/questions/24050923/slickgrid-mouseleave-event-not-fired-when-row-invalidated-after-mouseenter-f
		grid.onMouseEnter.subscribe(function(e, args){
		    const cell = grid.getCellFromEvent(e);
			if (!cell) {
				return;
			}

			// https://stackoverflow.com/questions/19701048/slickgrid-getting-selected-cell-value-id-and-field
			var item = grid.getDataItem(cell.row);
			console.debug(item);
			
		         const param = {};
		        const columnCss = {};

		      for(const column in grid.columns){
		          var id = column.id;
				  // https://stackoverflow.com/questions/15327990/generate-random-color-with-pure-css-no-javascript
		          columnCss[id] = 'my_highlighter_style'
		      }
		      param[cell.row] = columnCss;
		      args.grid.setCellCssStyles("row_highlighter", param);
		  })
	}
};

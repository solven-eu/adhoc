import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import { useUserStore } from "./store-user.js";

// Ordering of rows
import _ from "lodashEs";

import Sortable from "sortablejs";

// https://github.com/SortableJS/Sortable/issues/1229#issuecomment-521951729
window.Sortable = Sortable;

// https://stackoverflow.com/questions/59605033/how-to-self-reference-nodejs-module
const isSortable = function () {
	// Do not allow sorting until it is compatible with rowSpans
	return false;
};

const formatters = function (formatOptions) {
	if (!formatOptions) {
		formatOptions = {};
	}

	// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Intl/NumberFormat
	const numberFormatOptions = {};
	numberFormatOptions.maximumSignificantDigits = formatOptions.measureMaxDigits;
	if (formatOptions.measureCcy) {
		numberFormatOptions.style = "currency";
		numberFormatOptions.currency = formatOptions.measureCcy;
	}
	const numberFormat = new Intl.NumberFormat(formatOptions.locale, numberFormatOptions);

	function measureFormatter(row, cell, value, columnDef, dataContext) {
		var rtn = {};

		// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Data_structures
		if (typeof value === "number") {
			rtn.text = numberFormat.format(value);
		} else {
			rtn.text = value;
		}
		rtn.toolTip = value;

		return rtn;
	}

	const percentFormatOptions = {};
	percentFormatOptions.maximumSignificantDigits = formatOptions.measureMaxDigits;
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
				return item[0][column];
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
				cssClass: "text-end",
			};

			// BEWARE For an unknwon reason, this does not apply.
			// The css is added by SlickGrid, but the header is not aligned to the right
			column.headerCssClass = "text-end";

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
		console.log("columns", grid.getColumns());
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
};

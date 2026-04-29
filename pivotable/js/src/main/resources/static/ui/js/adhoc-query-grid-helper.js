import { ref, inject } from "vue";

// Ordering of rows. Per-function imports keep the browser from fetching the lodash root bundle —
// only the small graph behind each function is loaded.
import map from "lodashEs/map.js";
import orderBy from "lodashEs/orderBy.js";
import isEqual from "lodashEs/isEqual.js";
import sortBy from "lodashEs/sortBy.js";

import Sortable from "sortablejs";

// Formatters
import { SlickHeaderButtons } from "slickgrid";

// BEWARE: Should probably push an event to the Modal component so it open itself
import { Modal } from "bootstrap";

import { computeMeasureStats, computeParentSliceStats, heatmapColor, secondaryHeatmapFill } from "./adhoc-query-grid-heatmap.js";
import { headerNameWithCopyIcon, registerCopyNameDelegation } from "./adhoc-query-grid-clipboard.js";

// https://github.com/SortableJS/Sortable/issues/1229#issuecomment-521951729
window.Sortable = Sortable;

// https://stackoverflow.com/questions/59605033/how-to-self-reference-nodejs-module
const isSortable = function () {
	// Do not allow sorting until it is compatible with rowSpans
	return true;
};

// Copy a column / measure name to the system clipboard. Wired to the `bi-clipboard`
// icon next to each header name. We need an explicit copy affordance because clicking
// the column header itself triggers a sort (SlickGrid behaviour) — selecting the text
// with the mouse to copy-paste is therefore clumsy. Falls back to a manual textarea +
// document.execCommand("copy") when navigator.clipboard is unavailable (e.g. when
// served over plain HTTP without secure context).
const copyColumnNameToClipboard = function (name) {
	if (navigator.clipboard && navigator.clipboard.writeText) {
		navigator.clipboard.writeText(name).catch((e) => console.error("Clipboard write failed", e));
		return;
	}
	const ta = document.createElement("textarea");
	ta.value = name;
	ta.style.position = "fixed";
	ta.style.opacity = "0";
	document.body.appendChild(ta);
	ta.select();
	try {
		document.execCommand("copy");
	} catch (e) {
		console.error("execCommand copy failed", e);
	}
	document.body.removeChild(ta);
};

const formatters = function (formatOptions, measureStats, parentSliceStats, parentColumnNames) {
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

	// Build a heatmap-styled DOM node for a single numeric cell. SlickGrid's formatter
	// pipeline renders FormatterResultWithText via `textContent`, so any background color
	// MUST be attached to a DOM element returned via `.html`. We keep `display: block` so
	// the span fills the cell width, making the gradient visible across the whole cell
	// rather than just behind the digits.
	//
	// `formattedText` lets the caller plug in either the regular number format or the
	// percent format — sharing this builder across both formatters keeps the secondary-
	// heatmap rendering identical for both kinds of measure cells.
	function buildHeatmapCell(value, color, formattedText, dataContext, columnDef) {
		const el = document.createElement("span");
		el.style.display = "block";
		el.style.position = "relative";
		if (color) {
			el.style.backgroundColor = color;
		}

		// Secondary heatmap: HORIZONTAL bar spanning the FULL cell height, calibrated against
		// the PARENT-SLICE min/max (parent = the row's groupBy values minus the last view
		// column). The bar fills proportionally from the left edge — full-height makes the
		// signal much more visible than a thin strip, while the translucent fill keeps the
		// primary heatmap underneath legible.
		const parentBucket = parentSliceStats ? parentSliceStats[columnDef.id] : null;
		if (parentBucket && dataContext && parentColumnNames && parentColumnNames.length > 0) {
			const parts = [];
			for (const col of parentColumnNames) {
				parts.push(dataContext[col]);
			}
			const parentStats = parentBucket.get(JSON.stringify(parts));
			const fill = secondaryHeatmapFill(value, parentStats);
			if (fill !== null) {
				const bar = document.createElement("span");
				bar.style.position = "absolute";
				bar.style.left = "0";
				bar.style.top = "0";
				bar.style.bottom = "0";
				bar.style.width = (fill * 100).toFixed(1) + "%";
				bar.style.background = "rgba(0, 123, 255, 0.25)";
				bar.style.pointerEvents = "none";
				el.appendChild(bar);
			}
		}

		// Text node sits above the secondary bar so the digits remain readable on top of the
		// translucent fill. `position: relative` puts it on a higher stacking layer than the
		// absolutely-positioned bar (which is the cell's first child).
		const textNode = document.createElement("span");
		textNode.style.position = "relative";
		textNode.textContent = formattedText;
		el.appendChild(textNode);
		return el;
	}

	function measureFormatter(row, cell, value, columnDef, dataContext) {
		var rtn = {};

		// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Data_structures
		if (typeof value === "number") {
			const color = measureStats ? heatmapColor(value, measureStats[columnDef.id]) : null;
			// Build the cell DOM whenever EITHER the primary heatmap colours it, OR the secondary
			// heatmap has a bar to render (parent-slice stats define a fill). This keeps the
			// secondary bar visible even when the primary heatmap is degenerate (single value or
			// midpoint-equal — `heatmapColor` returns null then).
			const hasSecondary = parentSliceStats && parentSliceStats[columnDef.id] && parentColumnNames && parentColumnNames.length > 0;
			if (color || hasSecondary) {
				rtn.html = buildHeatmapCell(value, color, numberFormat.format(value), dataContext, columnDef);
				rtn.toolTip = value;
				return rtn;
			}
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

	// Adaptive percent format: each measure column gets its own Intl.NumberFormat whose
	// `maximumFractionDigits` is chosen from the column's observed |min|/|max| so the user always sees
	// at least ~2 significant digits at the column's actual scale. Without this, a measure whose values
	// are all under 1% (e.g. `% delta / (delta+gamma)` returning 0.001..0.005) renders as "0%" / "0.0%"
	// across the whole column — pointless data.
	//
	// Bucketing rule (computed against the larger of |stats.min| and |stats.max|, expressed as a percent):
	//   ≥ 10%  → 0 fraction digits ("50%")
	//   ≥ 1%   → 1 fraction digit  ("5.5%")
	//   < 1%   → ceil(-log10(maxPct)) + 2 fraction digits, capped at 6
	//             (e.g. 0.005% → 4 fraction digits → "0.0050%")
	// Defaults to 2 fraction digits when measureStats is absent (e.g. one-row queries with no spread).
	const percentFormatPerColumn = new Map();
	function getPercentFormat(columnId) {
		let fmt = percentFormatPerColumn.get(columnId);
		if (fmt) return fmt;
		let maxFracDigits = 2;
		const stats = measureStats ? measureStats[columnId] : null;
		if (stats && stats.count > 0) {
			const maxAbs = Math.max(Math.abs(stats.min), Math.abs(stats.max));
			if (maxAbs > 0) {
				const maxPct = maxAbs * 100;
				if (maxPct >= 10) {
					maxFracDigits = 0;
				} else if (maxPct >= 1) {
					maxFracDigits = 1;
				} else {
					maxFracDigits = Math.min(6, Math.max(2, Math.ceil(-Math.log10(maxPct)) + 2));
				}
			}
		}
		fmt = new Intl.NumberFormat(formatOptions.locale, {
			style: "percent",
			minimumFractionDigits: maxFracDigits,
			maximumFractionDigits: maxFracDigits,
		});
		percentFormatPerColumn.set(columnId, fmt);
		return fmt;
	}

	// https://github.com/6pac/SlickGrid/blob/master/src/slick.formatters.ts
	function percentFormatter(row, cell, value, columnDef, dataContext) {
		var rtn = {};
		const percentFormat = getPercentFormat(columnDef.id);

		// Apply the primary + secondary heatmaps to percent-formatted cells the same way
		// `measureFormatter` does — they share the same numeric semantics, and any measure whose
		// name contains `%` is routed here.
		if (typeof value === "number") {
			const color = measureStats ? heatmapColor(value, measureStats[columnDef.id]) : null;
			const hasSecondary = parentSliceStats && parentSliceStats[columnDef.id] && parentColumnNames && parentColumnNames.length > 0;
			if (color || hasSecondary) {
				rtn.html = buildHeatmapCell(value, color, percentFormat.format(value), dataContext, columnDef);
				rtn.toolTip = value;
				return rtn;
			}
		}

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
	computeMeasureStats,
	computeParentSliceStats,
	heatmapColor,

	sortRows: function (columnNames, coordinates, values) {
		if (coordinates.length != values.length) {
			throw new Error("Incomptable length: coordinates.length=" + coordinates.length + " values.length=" + values.length);
		}

		// https://stackoverflow.com/questions/48701488/how-to-order-array-by-another-array-ids-lodash-javascript
		const index = map(coordinates, (x, i) => [coordinates[i], values[i]]);

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
		const indexSorted = orderBy(index, sortingFunctions, sortingOrders);

		for (let i = 0; i < coordinates.length; i++) {
			coordinates[i] = indexSorted[i][0];
			values[i] = indexSorted[i][1];
		}
	},

	sanityCheckFirstRow: function (columnNames, coordinatesRow, measureNames, measuresRow) {
		// https://stackoverflow.com/questions/29951293/using-lodash-to-compare-jagged-arrays-items-existence-without-order
		if (!isEqual(sortBy(columnNames), sortBy(Object.keys(coordinatesRow)))) {
			throw new Error(`Inconsistent columnNames: ${columnNames} vs ${Object.keys(coordinatesRow)}`);
		}
		if (!isEqual(sortBy(measureNames), sortBy(Object.keys(measuresRow)))) {
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
				name: headerNameWithCopyIcon(columnName),
				field: columnName,
				sortable: isSortable(),
				asyncPostRender: renderCallback,
				// formatter: popoverFormatter,
			};

			if (queryModel) {
				// queryModel is available: show a button to edit the queryModel from the grid.
				// Note: the copy-name affordance lives INLINE in `column.name` (above) so the
				// icon sits right next to the name rather than at the far end of the header
				// alongside the other action buttons.
				column.header = {
					buttons: [
						{
							command: "remove-column",
							tooltip: "Remove this groupBy",
							cssClass: "bi bi-x-circle",
							itemVisibilityOverride: function (args) {
								// for example don't show the header button on column "E"
								return args.column.id !== "E";
							},
							itemUsabilityOverride: function (args) {
								// for example the button usable everywhere except on last column "J"
								return args.column.id !== "J";
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
								return args.column.id !== "E";
							},
							itemUsabilityOverride: function (args) {
								// for example the button usable everywhere except on last column "J"
								return args.column.id !== "J";
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

	measuresToGridColumns: function (measureNames, queryModel, renderCallback, formatOptions, measureStats, parentSliceStats, parentColumnNames) {
		const measureFormatters = formatters(formatOptions, measureStats, parentSliceStats, parentColumnNames);

		const gridColumns = [];

		for (let measureName of measureNames) {
			const column = {
				id: measureName,
				name: headerNameWithCopyIcon(measureName),
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
				// queryModel is available: show a button to edit the queryModel from the grid.
				// Note: the copy-name affordance lives INLINE in `column.name` (above) so the
				// icon sits right next to the name rather than at the far end of the header.
				// The Statistics affordance lives in the FOOTER (next to min/max), not here.
				column.header = {
					buttons: [
						{
							command: "remove-measure",
							tooltip: "Remove this measure",
							cssClass: "bi bi-x-circle",
							itemVisibilityOverride: function (args) {
								// for example don't show the header button on column "E"
								return args.column.id !== "E";
							},
							itemUsabilityOverride: function (args) {
								// for example the button usable everywhere except on last column "J"
								return args.column.id !== "J";
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
								return args.column.id !== "E";
							},
							itemUsabilityOverride: function (args) {
								// for example the button usable everywhere except on last column "J"
								return args.column.id !== "J";
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

	updateFooters: function (grid, columnNames, coordinates, values, measureStats, formatOptions) {
		// Update footer row
		const columnToDistinctCount = {};

		// Footer numbers reuse the same Intl.NumberFormat the measure cells use, so the
		// summary (min / sum / max) visually lines up with the cell values above it. We
		// build a dedicated formatter (no heatmap) so the footer stays plain text.
		const footerFormatters = formatters(formatOptions || {});
		const formatNumber = function (n) {
			if (n === null || n === undefined || Number.isNaN(n)) {
				return "";
			}
			// Borrow the measure number-format by calling the same formatter with a fake cell.
			const out = footerFormatters.measureFormatter(0, 0, n, { id: "__footer__" }, {});
			return out && typeof out.text === "string" ? out.text : String(n);
		};

		for (let column of grid.getColumns()) {
			// `column.name` is now HTML (the inline name + copy-icon markup produced by
			// `headerNameWithCopyIcon`), so we MUST key off `column.id` here — that is the
			// bare identifier the rest of the model and the `coordinates[row]` map index by.
			// A previous version used `column.name` and silently dropped every groupBy footer
			// (the `columnNames.includes(...)` test never matched HTML).
			const columnId = column.id;

			// Always reset the footer cell up-front so a stale "min … · max …" or stale
			// Statistics button from a previous render does not leak into a column whose
			// current contract no longer warrants one (e.g. a measure column whose values
			// turn out to be all-strings, leaving `s.count === 0` and producing no footerText).
			const columnElement = grid.getFooterRowColumn(column.id);
			if (columnElement) {
				columnElement.textContent = "";
			}

			var footerText = null;
			if ("id" === column.id) {
				// rowIndex column has `distinctCount==length`
				columnToDistinctCount[columnId] = coordinates.length;

				footerText = `#: ${columnToDistinctCount[columnId]}`;
			} else if (columnNames.includes(columnId)) {
				const values = [];

				for (let rowIndex = 0; rowIndex < coordinates.length; rowIndex++) {
					values.push(coordinates[rowIndex][columnId]);
				}

				// https://stackoverflow.com/questions/21661686/fastest-way-to-get-count-of-unique-elements-in-javascript-array
				columnToDistinctCount[columnId] = new Set(values).size;

				footerText = `#: ${columnToDistinctCount[columnId]}`;
			} else if (measureStats && measureStats[column.id]) {
				// Measure column: show min adjacent to max so the eye can compare the heatmap
				// endpoints at a glance. Sum / mean / variance / null counts moved to the
				// per-column Statistics modal — accessible via the footer button below.
				const s = measureStats[column.id];
				if (s.count > 0) {
					footerText = `min ${formatNumber(s.min)} · max ${formatNumber(s.max)}`;
				} else {
					// All values are non-numeric (e.g. a custom-marker measure surfacing
					// currency codes). Fall back to the distinct-count footer the groupBy
					// branch uses — same useful summary, just without min/max which would
					// be meaningless on strings.
					const measureValues = [];
					for (let rowIndex = 0; rowIndex < values.length; rowIndex++) {
						measureValues.push(values[rowIndex][columnId]);
					}
					footerText = `#: ${new Set(measureValues).size}`;
				}
			}

			if (footerText) {
				// https://github.com/6pac/SlickGrid/blob/master/examples/example-footer-totals.html
				columnElement.textContent = footerText;
				// Append a Statistics affordance to MEASURE columns only, anchored next to
				// min/max in the footer where the related summary numbers already live.
				// `setAttribute("data-adhoc-stats-measure", ...)` lets the registered
				// click-delegation handler (in `registerHeaderButtons`) open the modal
				// without us having to keep a per-button reference around. Skipped for the
				// all-non-numeric fallback branch — the stats modal would show count=0 / sum=0
				// and offer no useful information for string-valued measures.
				if (measureStats && measureStats[column.id] && measureStats[column.id].count > 0) {
					const btn = document.createElement("i");
					btn.className = "bi bi-bar-chart adhoc-stats-btn";
					btn.setAttribute("role", "button");
					btn.setAttribute("tabindex", "0");
					btn.setAttribute("title", "Statistics for this measure");
					btn.setAttribute("data-adhoc-stats-measure", column.id);
					btn.style.cursor = "pointer";
					btn.style.marginLeft = "0.5rem";
					btn.style.opacity = "0.6";
					columnElement.appendChild(document.createTextNode(" "));
					columnElement.appendChild(btn);
				}
			}
		}
	},

	registerHeaderButtons(grid, queryModel) {
		// https://github.com/6pac/SlickGrid/blob/master/examples/example-plugin-headerbuttons.html
		var headerButtonsPlugin = new SlickHeaderButtons();

		// Inline copy-name icon — rendered as part of `column.name` HTML so it sits next to
		// the name itself (not at the far right of the header where SlickHeaderButtons drops
		// its icons). Click delegation lives in `adhoc-query-grid-clipboard.js` and uses
		// CAPTURE phase so SlickGrid's sort handler (bound on `.slick-header-column` in
		// bubble phase) does not fire when the user clicks the icon.
		const containerEl = grid.getContainerNode();
		registerCopyNameDelegation(containerEl, copyColumnNameToClipboard);

		const ids = inject("ids");

		// a ref to create a single Modal object
		const measuresDagModal = new Modal(document.getElementById("measureDag"), {});
		const measuresDagModel = inject("measuresDagModel");

		const columnFilterModal = new Modal(document.getElementById("columnFilterModal"), {});
		const columnFilterModel = inject("columnFilterModel");

		// Per-measure Statistics modal — singleton, mounted by `adhoc-query-grid.js`. The
		// `allStats` map is refreshed on every grid resync; this dispatcher just selects
		// the active measure and shows the modal.
		const measureStatsEl = document.getElementById("measureStatsModal");
		const measureStatsModal = measureStatsEl ? new Modal(measureStatsEl, {}) : null;
		const measureStatsModel = inject("measureStatsModel", null);

		// All action callbacks read `column.id` rather than `column.name` because
		// `column.name` now contains the rendered HTML (name + inline copy icon) — see
		// `headerNameWithCopyIcon`. `column.id` is still the bare identifier the rest of
		// the model keys off.
		headerButtonsPlugin.onCommand.subscribe(function (e, args) {
			var column = args.column;
			var button = args.button;
			var command = args.command;

			if (command == "remove-column") {
				queryModel.selectedColumns[column.id] = false;
				queryModel.onColumnToggled(column.id);

				// No need to invalidate the grid, as the queryModel change shall trigger a grid/tabularView/data update
				// grid.invalidate();
			} else if (command == "filter-column") {
				columnFilterModel.column = column.id;
				columnFilterModal.show();
			} else if (command == "remove-measure") {
				queryModel.selectedMeasures[column.id] = false;

				// No need to invalidate the grid, as the queryModel change shall trigger a grid/tabularView/data update
				// grid.invalidate();
			} else if (command == "info-measure") {
				console.log("Info measure", column.id);

				measuresDagModel.main = column.id;
				measuresDagModal.show();
			}
		});

		// Footer-side click delegation — the Statistics button lives in the per-measure
		// footer cell (rendered by `updateFooters`), not in the header. We piggy-back on
		// the same container we wired above for the inline copy-name icon.
		if (containerEl && !containerEl.__adhocStatsBtnWired) {
			containerEl.__adhocStatsBtnWired = true;
			containerEl.addEventListener("click", function (e) {
				const btn = e.target && e.target.closest && e.target.closest(".adhoc-stats-btn");
				if (!btn) return;
				e.preventDefault();
				e.stopPropagation();
				const measure = btn.getAttribute("data-adhoc-stats-measure") || "";
				if (!measureStatsModel || !measureStatsModal) return;
				measureStatsModel.measureName = measure;
				measureStatsModel.stats = (measureStatsModel.allStats && measureStatsModel.allStats[measure]) || null;
				measureStatsModal.show();
			});
		}

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
		grid.onMouseEnter.subscribe(function (e, args) {
			const cell = grid.getCellFromEvent(e);
			if (!cell) {
				return;
			}

			// https://stackoverflow.com/questions/19701048/slickgrid-getting-selected-cell-value-id-and-field
			var item = grid.getDataItem(cell.row);
			console.debug(item);

			const param = {};
			const columnCss = {};

			for (const column in grid.columns) {
				var id = column.id;
				// https://stackoverflow.com/questions/15327990/generate-random-color-with-pure-css-no-javascript
				columnCss[id] = "my_highlighter_style";
			}
			param[cell.row] = columnCss;
			args.grid.setCellCssStyles("row_highlighter", param);
		});
	},
};

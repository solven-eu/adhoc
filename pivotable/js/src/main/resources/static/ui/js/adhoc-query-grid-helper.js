// @ts-check
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
import { keepOnlyMeasure } from "./adhoc-query-keep-only-measure.js";
import { registerHeaderResizeAutoFit } from "./adhoc-query-grid-autofit.js";
import { extractCellText, isCopyShortcut } from "./adhoc-query-grid-copy-cell.js";

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

// Cell-value placeholders rendered in greyed italic to distinguish "no data" from "blank". Apply to every
// view (not just DRILLTHROUGH):
//
//   - `NULL`        : the cell value is null/undefined. A blank cell would be ambiguous with an empty string,
//                     so we make the null state explicit.
//   - `empty`       : the cell value is the empty string. Symmetric to `NULL` — makes it clear the source
//                     data is "" rather than missing.
//   - `Out of DT`   : DRILLTHROUGH-only. The column is NOT in the user's query — it appeared in the DT
//                     response because the engine decomposed a higher-level measure (e.g. a Combinator) into
//                     its underlying aggregator columns, or because a row-inclusion filter column was added
//                     to the groupBy. The user did not explicitly request this column, so a missing/empty
//                     value really means "this column is not part of the DT contract for this row".
//
// Sanitised inline (no user-controlled content) so it is safe to emit as raw HTML.
const NULL_HTML = '<span style="color:#888;font-style:italic">NULL</span>';
const NULL_TOOLTIP = "The cell carries a null value.";
const EMPTY_HTML = '<span style="color:#888;font-style:italic">empty</span>';
const EMPTY_TOOLTIP = "The cell carries an empty string.";
const OUT_OF_DT_HTML = '<span style="color:#888;font-style:italic">Out of DT</span>';
const OUT_OF_DT_TOOLTIP = "This column is not part of the user's DrillThrough query.";

// `*` is the engine-side marker for a grand-total rollup (CalculatedCoordinate). The query payload keeps
// `*` as the literal coordinate; only the rendering swaps it for the human-friendly `Total` label so the
// asterisk is not read as a real coordinate value. Hardcoded for now — could become a pivotable parameter
// later (Excel uses "Grand Total" for the all-columns rollup). BEWARE: a real data row carrying the literal
// coordinate `*` is visually indistinguishable from the synthetic grand-total — documented limitation,
// see query-grid-formatters.spec.js for the pinned behaviour.
const TOTAL_LABEL = "Total";
const TOTAL_TOOLTIP = "Grand-total rollup for this coordinate.";

function isMissing(value) {
	return value === null || typeof value === "undefined";
}

// Returns a SlickGrid cell descriptor when `value` should be rendered as a special missing/empty marker,
// else null (caller falls through to its normal formatting). Logic:
//
//   - DT mode + column NOT in `requestedColumns` + value missing/empty  → "Out of DT"
//   - value null/undefined                                              → "NULL"
//   - value === ""                                                      → "empty"
//   - otherwise                                                         → null (no placeholder)
//
// `requestedColumns` may be undefined outside DT mode; it is consulted only when `isDrillthrough` is true.
function placeholderForCellValue(columnId, value, requestedColumns, isDrillthrough) {
	const isMissingValue = isMissing(value);
	const isEmptyString = value === "";
	if (!isMissingValue && !isEmptyString) {
		return null;
	}
	const isOutOfDT = isDrillthrough && requestedColumns && !requestedColumns.has(columnId);
	if (isOutOfDT) {
		return { html: OUT_OF_DT_HTML, toolTip: OUT_OF_DT_TOOLTIP };
	}
	if (isMissingValue) {
		return { html: NULL_HTML, toolTip: NULL_TOOLTIP };
	}
	return { html: EMPTY_HTML, toolTip: EMPTY_TOOLTIP };
}

const formatters = function (formatOptions, measureStats, parentSliceStats, parentColumnNames, isDrillthrough, requestedColumns) {
	if (!formatOptions) {
		formatOptions = {};
	}

	// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Intl/NumberFormat
	// `Intl.NumberFormatOptions & any` so newer keys (`roundingPriority`) that may be missing from the
	// installed TypeScript lib still type-check — the runtime accepts them on all evergreen browsers.
	/** @type {Intl.NumberFormatOptions & Record<string, any>} */
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

	// Per-column override: when every observed numeric value in a column is an integer
	// (`measureStats[columnId].allInteger`), drop the fractional part so the user doesn't see
	// "12.00" on a count column. The override is cached per columnId since `Intl.NumberFormat`
	// is not the cheapest object to construct on every cell render. Falls back to the shared
	// `numberFormat` when no stats are available or the column has fractional values.
	const integerFormatPerColumn = new Map();
	function formatNumber(columnId, value) {
		const stats = measureStats ? measureStats[columnId] : null;
		if (stats && stats.count > 0 && stats.allInteger) {
			let fmt = integerFormatPerColumn.get(columnId);
			if (!fmt) {
				// Build the integer-format options from scratch rather than spreading
				// `numberFormatOptions` — the spread used to carry through `undefined` keys
				// (notably `roundingPriority` and `maximumSignificantDigits`) which some
				// `Intl.NumberFormat` implementations treat as "explicitly set to default"
				// and then conflict with the fraction-digits 0/0 pair, silently downgrading
				// the format to the locale default and showing "12.00" again. Carrying only
				// the keys we actually want avoids the foot-gun.
				/** @type {Intl.NumberFormatOptions & Record<string, any>} */
				const intOptions = { minimumFractionDigits: 0, maximumFractionDigits: 0 };
				if (numberFormatOptions.style === "currency") {
					intOptions.style = "currency";
					intOptions.currency = numberFormatOptions.currency;
				}
				try {
					fmt = new Intl.NumberFormat(formatOptions.locale, intOptions);
				} catch (e) {
					fmt = numberFormat;
				}
				integerFormatPerColumn.set(columnId, fmt);
			}
			return fmt.format(value);
		}
		return numberFormat.format(value);
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

		// Missing / empty cell rendering: greyed italic `NULL` for null/undefined, `empty` for "". Applies
		// to every view, not just DT. DT additionally distinguishes columns the user did not request
		// ("Out of DT") — handled inside placeholderForCellValue.
		const placeholder = placeholderForCellValue(columnDef.id, value, requestedColumns, isDrillthrough);
		if (placeholder) {
			return placeholder;
		}

		// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Data_structures
		if (typeof value === "number") {
			// Two independent toggles in the Formatting Options modal — both default OFF, so a
			// fresh page-load shows plain text. Each gate short-circuits its respective helper
			// call so the disabled feature costs nothing.
			const primaryOn = !!formatOptions.primaryHeatmap;
			const secondaryOn = !!formatOptions.secondaryHeatmap;
			const color = primaryOn && measureStats ? heatmapColor(value, measureStats[columnDef.id]) : null;
			// Build the cell DOM whenever EITHER the primary heatmap colours it, OR the secondary
			// heatmap has a bar to render (parent-slice stats define a fill). This keeps the
			// secondary bar visible even when the primary heatmap is degenerate (single value or
			// midpoint-equal — `heatmapColor` returns null then).
			const hasSecondary = secondaryOn && parentSliceStats && parentSliceStats[columnDef.id] && parentColumnNames && parentColumnNames.length > 0;
			if (color || hasSecondary) {
				rtn.html = buildHeatmapCell(value, color, formatNumber(columnDef.id, value), dataContext, columnDef);
				rtn.toolTip = value;
				return rtn;
			}
			rtn.text = formatNumber(columnDef.id, value);
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

		// Same missing/empty placeholder handling as `measureFormatter` — see comment there.
		const placeholder = placeholderForCellValue(columnDef.id, value, requestedColumns, isDrillthrough);
		if (placeholder) {
			return placeholder;
		}

		// Apply the primary + secondary heatmaps to percent-formatted cells the same way
		// `measureFormatter` does — they share the same numeric semantics, and any measure whose
		// name contains `%` is routed here. Gated on the two independent toggles.
		if (typeof value === "number") {
			const primaryOn = !!formatOptions.primaryHeatmap;
			const secondaryOn = !!formatOptions.secondaryHeatmap;
			const color = primaryOn && measureStats ? heatmapColor(value, measureStats[columnDef.id]) : null;
			const hasSecondary = secondaryOn && parentSliceStats && parentSliceStats[columnDef.id] && parentColumnNames && parentColumnNames.length > 0;
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

	groupByToGridColumns: function (columnNames, queryModel, renderCallback, isDrillthrough, requestedColumns) {
		const gridColumns = [];

		// Always-on formatter: renders missing/empty cells as `NULL` / `empty` placeholders, and the
		// engine-side grand-total marker `*` as the friendlier `Total` label. DRILLTHROUGH additionally
		// distinguishes columns the user did not request (`Out of DT`) — handled by placeholderForCellValue.
		const groupByFormatter = function (row, cell, value, columnDef /* , dataContext */) {
			const placeholder = placeholderForCellValue(columnDef.id, value, requestedColumns, isDrillthrough);
			if (placeholder) {
				return placeholder;
			}
			if (value === "*") {
				return { text: TOTAL_LABEL, toolTip: TOTAL_TOOLTIP };
			}
			return { text: value, toolTip: value };
		};

		for (let columnName of columnNames) {
			const column = {
				id: columnName,
				name: headerNameWithCopyIcon(columnName),
				field: columnName,
				sortable: isSortable(),
				asyncPostRender: renderCallback,
				formatter: groupByFormatter,
			};

			if (queryModel) {
				// Single 3-dot menu in the header — replaces the previous pair of separate
				// remove-column / filter-column icons. Stacking destructive icons next to the
				// column's resize handle was error-prone (one mis-aimed click and the column
				// disappeared). The menu is populated dynamically in `registerHeaderButtons` by
				// reading `column.__menuItems`.
				column.__menuItems = [
					{ label: "Filter…", icon: "bi-filter-circle", command: "filter-column" },
					{ label: "Remove from groupBy", icon: "bi-x-circle", command: "remove-column", destructive: true },
				];
				column.header = {
					buttons: [
						{
							command: "column-menu",
							tooltip: "Column actions",
							cssClass: "bi bi-three-dots-vertical",
						},
					],
				};
			}

			gridColumns.push(column);
		}

		return gridColumns;
	},

	measuresToGridColumns: function (
		measureNames,
		queryModel,
		renderCallback,
		formatOptions,
		measureStats,
		parentSliceStats,
		parentColumnNames,
		isDrillthrough,
		requestedColumns,
	) {
		const measureFormatters = formatters(formatOptions, measureStats, parentSliceStats, parentColumnNames, isDrillthrough, requestedColumns);

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
				// Single 3-dot menu — same design as for groupBy columns. The menu is populated
				// dynamically in `registerHeaderButtons` by reading `column.__menuItems`.
				column.__menuItems = [
					{ label: "Show DAG", icon: "bi-question-circle", command: "info-measure" },
					// "Keep only" is the pivot-tool name for narrowing to a single item; it is the
					// bulk complement of "Remove measure" when a query carries many measures.
					// Destructive for the same reason as removal, and more so: it deselects every
					// other measure rather than one.
					{ label: "Keep only this measure", icon: "bi-bullseye", command: "keep-only-measure", destructive: true },
					{ label: "Remove measure", icon: "bi-x-circle", command: "remove-measure", destructive: true },
				];
				column.header = {
					buttons: [
						{
							command: "column-menu",
							tooltip: "Measure actions",
							cssClass: "bi bi-three-dots-vertical",
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
		//
		// measureStats MUST be threaded through here too — otherwise the footer's
		// `formatNumber` has no way to detect an all-integer column and the min/max line
		// shows e.g. "min 5.00 · max 12.00" while the cells above (which DO see stats)
		// render the same numbers as "5" and "12". Visual inconsistency that prompted the
		// "default format does not print decimals" complaint.
		const footerFormatters = formatters(formatOptions || {}, measureStats);
		const formatNumber = function (n, columnId) {
			if (n === null || n === undefined || Number.isNaN(n)) {
				return "";
			}
			// Borrow the measure number-format by calling the same formatter with the REAL column id
			// — the integer-format path in `formatters()` keys off it (`stats[columnId].allInteger`),
			// so a synthetic id would lose the per-column override and render the footer's min/max
			// with decimals while the cells above show integers.
			const out = footerFormatters.measureFormatter(0, 0, n, { id: columnId || "__footer__" }, {});
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
			// Also strip the alignment classes — a measure column that lost its numeric stats
			// on a re-render should fall back to the default left alignment used by groupBys.
			const columnElement = grid.getFooterRowColumn(column.id);
			if (columnElement) {
				columnElement.textContent = "";
				columnElement.classList.remove("text-end", "font-monospace");
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
					footerText = `min ${formatNumber(s.min, column.id)} · max ${formatNumber(s.max, column.id)}`;
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
				// Mirror the header alignment for measure columns — the header uses
				// `text-end justify-content-end` so the column name sits on the right edge;
				// applying `text-end font-monospace` to the footer cell makes "min … · max …"
				// (and the Statistics affordance) line up with the numeric values above.
				// groupBy / rowIndex footers keep the default left alignment since their
				// `#: N` summary reads naturally from the left.
				if (measureStats && measureStats[column.id]) {
					columnElement.classList.add("text-end", "font-monospace");
				}
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
		// Internal helper used by the per-column 3-dot menu. Builds a small floating dropdown next
		// to the icon and dispatches each item's command through the caller-provided callback. Pure
		// DOM (no Bootstrap Dropdown wiring) so we can position it precisely without retrofitting
		// the SlickGrid header markup with a `data-bs-toggle` parent.
		// eslint-disable-next-line no-inner-declarations
		function showColumnHeaderMenu(anchorEl, items, onPick) {
			// Tear down any prior menu so a second click on a different column's icon does not
			// leave two menus floating.
			document.querySelectorAll(".adhoc-column-menu").forEach((el) => el.remove());

			const menu = document.createElement("div");
			menu.className = "adhoc-column-menu dropdown-menu show shadow-sm";
			menu.style.position = "absolute";
			menu.style.zIndex = "1050";

			for (const item of items) {
				const btn = document.createElement("button");
				btn.type = "button";
				btn.className = "dropdown-item d-flex align-items-center" + (item.destructive ? " text-danger" : "");
				btn.innerHTML = `<i class="bi ${item.icon} me-2"></i><span>${item.label}</span>`;
				btn.addEventListener("click", function (e) {
					e.preventDefault();
					e.stopPropagation();
					menu.remove();
					onPick(item.command);
				});
				menu.appendChild(btn);
			}

			document.body.appendChild(menu);

			// Position the menu just under the icon, right-aligned so it doesn't push past the
			// viewport's right edge.
			const rect = anchorEl.getBoundingClientRect();
			const menuWidth = menu.offsetWidth;
			const top = rect.bottom + window.scrollY + 2;
			let left = rect.right + window.scrollX - menuWidth;
			if (left < 8) left = 8;
			menu.style.top = top + "px";
			menu.style.left = left + "px";

			// Click anywhere outside the menu → dismiss. Capture phase so we run before any
			// bubble-phase handler that might also react to the click.
			const onOutside = function (e) {
				if (!menu.contains(e.target)) {
					menu.remove();
					document.removeEventListener("click", onOutside, true);
					document.removeEventListener("keydown", onEsc, true);
				}
			};
			const onEsc = function (e) {
				if (e.key === "Escape") {
					menu.remove();
					document.removeEventListener("click", onOutside, true);
					document.removeEventListener("keydown", onEsc, true);
				}
			};
			// Defer the listener wiring one tick so the click that opened the menu doesn't
			// immediately close it.
			setTimeout(() => {
				document.addEventListener("click", onOutside, true);
				document.addEventListener("keydown", onEsc, true);
			}, 0);
		}

		// https://github.com/6pac/SlickGrid/blob/master/examples/example-plugin-headerbuttons.html
		// SlickHeaderButtons 5.x expects an options arg; the runtime accepts an empty object as defaults.
		var headerButtonsPlugin = new SlickHeaderButtons({});

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

		// Per-column action dispatcher. Shared between the legacy `onCommand` (in case some
		// pre-existing call site still fires a direct command) and the new 3-dot-menu items.
		const dispatchColumnCommand = function (command, column) {
			if (command === "remove-column") {
				queryModel.selectedColumns[column.id] = false;
				queryModel.onColumnToggled(column.id);
			} else if (command === "filter-column") {
				columnFilterModel.column = column.id;
				columnFilterModal.show();
			} else if (command === "remove-measure") {
				queryModel.selectedMeasures[column.id] = false;
			} else if (command === "keep-only-measure") {
				keepOnlyMeasure(queryModel.selectedMeasures, column.id);
			} else if (command === "info-measure") {
				measuresDagModel.main = column.id;
				measuresDagModal.show();
			}
		};

		// All action callbacks read `column.id` rather than `column.name` because
		// `column.name` now contains the rendered HTML (name + inline copy icon) — see
		// `headerNameWithCopyIcon`. `column.id` is still the bare identifier the rest of
		// the model keys off.
		headerButtonsPlugin.onCommand.subscribe(function (e, args) {
			var column = args.column;
			var command = args.command;

			if (command === "column-menu") {
				// The 3-dot icon was clicked. Open a small dropdown anchored to the icon with the
				// column-specific actions declared on `column.__menuItems`.
				// `__menuItems` is a Pivotable-stamped extension on the SlickGrid column definition, see
				// `groupByToGridColumns` / `measuresToGridColumns` for the producers. SlickGrid's strict
				// `Column<T>` shape doesn't include it.
				const items = /** @type {any} */ (column).__menuItems || [];
				const anchor = e && e.target ? e.target.closest(".slick-header-button") || e.target : null;
				if (anchor && items.length > 0) {
					showColumnHeaderMenu(anchor, items, (cmd) => dispatchColumnCommand(cmd, column));
				}
				return;
			}

			// Legacy direct-command path — kept so any pre-existing caller still works.
			dispatchColumnCommand(command, column);
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
		// SlickGrid's `createColumnFooter` hardcodes the footer-cell className and ignores
		// `column.footerCssClass` (unlike `headerCssClass` / `cssClass` for header / body), so the
		// phantom column's footer cell would NOT carry our `.slick-footer-phantom` marker out of the
		// box. Stamp it ourselves from the per-cell rendered event — SlickGrid fires this
		// synchronously inside `createColumnFooter` after each footer cell is created, so our class
		// lands BEFORE the layout pass reads styles.
		grid.onFooterRowCellRendered.subscribe(function (e, args) {
			if (args.column && args.column.id === "__phantom_trailing" && args.node) {
				args.node.classList.add("slick-footer-phantom");
			}
		});

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

			// Single-click cell tracking for the Ctrl/Cmd+C copy-cell shortcut. We capture (row, columnId)
			// rather than the rendered value so the keyboard handler can re-read the dataView at copy-time
			// (covers the live-update case: a cell whose value mutates between click and Ctrl+C still
			// copies the CURRENT value, not the stale click-time snapshot).
			/** @type {{ row: number, columnId: string|number } | null} */
			let lastClickedCell = null;
			grid.onClick.subscribe(function (e, args) {
				const cols = grid.getColumns();
				if (args.cell < 0 || args.cell >= cols.length) return;
				lastClickedCell = { row: args.row, columnId: cols[args.cell].id };
			});

			// Ctrl+C / Cmd+C on a clicked cell copies its value. We listen on `grid.onKeyDown` rather than
			// `document.addEventListener("keydown", …)` so the shortcut only fires when the grid has focus —
			// pressing Ctrl+C while typing into the search box or with a text selection elsewhere keeps the
			// browser's default behaviour. `isCopyShortcut` additionally guards against editable targets.
			grid.onKeyDown.subscribe(function (e) {
				const ev = e && /** @type {any} */ (e).originalEvent ? /** @type {any} */ (e).originalEvent : e;
				if (!isCopyShortcut(/** @type {any} */ (ev))) return;
				if (!lastClickedCell) return;
				const item = dataView.getItem(lastClickedCell.row);
				if (!item) return;
				const text = extractCellText(item[lastClickedCell.columnId]);
				if (!text) return;
				if (!navigator.clipboard || !navigator.clipboard.writeText) {
					console.warn("navigator.clipboard not available (insecure context?) — cell value not copied");
					return;
				}
				navigator.clipboard.writeText(text).catch((err) => {
					console.error("Failed copying cell value via Ctrl+C:", err);
				});
				// Prevent the browser's default copy from also running — without preventDefault, a stray
				// text selection on the page would override our writeText with the selection's content.
				if (ev && typeof ev.preventDefault === "function") ev.preventDefault();
				if (e && typeof (/** @type {any} */ (e).stopPropagation) === "function") {
					/** @type {any} */ (e).stopPropagation();
				}
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
				// TODO Pre-existing bug: `for...in` makes `column` a string KEY, so `column.id` is always
				// undefined and `columnCss` ends up empty. The row-highlighter therefore applies no styles
				// in practice. Should be `for...of` over `grid.getColumns()` to iterate column DEFINITIONS,
				// but changing that here would enable a visual feature that has been dormant in prod for a
				// long time — leave the no-op behavior unchanged until a follow-up validates the intended UX.
				// @ts-ignore -- intentional: see TODO above.
				var id = column.id;
				// https://stackoverflow.com/questions/15327990/generate-random-color-with-pure-css-no-javascript
				columnCss[id] = "my_highlighter_style";
			}
			param[cell.row] = columnCss;
			args.grid.setCellCssStyles("row_highlighter", param);
		});

		// Excel-like auto-fit on dblclick of a column resize handle. See `adhoc-query-grid-autofit.js` for
		// the measurement strategy + unit tests.
		registerHeaderResizeAutoFit(grid, dataView);
	},
};

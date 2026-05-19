// @ts-check

// Pure substring search across a materialized TabularView. Intended for the navbar's
// application-level search modal — augments (does NOT replace) the browser's native Ctrl+F,
// which only finds matches in the lazy-rendered SlickGrid viewport.
//
// Matching contract (matches browser-search behaviour as closely as a JS-side helper can):
//   - Case-insensitive substring on the cell's displayed/formatted string.
//   - Also matches the raw `String(value)` so `1234` finds a cell whose raw value is `71234`
//     even though the formatter renders it as `71,234.00` — typing the digit sequence the
//     user remembers is enough, regardless of which side of the formatter their memory came
//     from.
//   - Null / undefined cells skip; non-string-non-number values fall back to `String(v)`.
//
// `formatCell(rowIndex, columnId) -> string | null` is the integration seam with SlickGrid's
// per-column formatters; pass `null` when no formatter is available and the helper compares
// against the raw string only. The function returns at most `limit` hits, in row-major /
// column-major order — caller decides whether to slice further for UI rendering.

/**
 * @typedef {Object} TabularViewSlice
 * @property {Array<Record<string, any>>} coordinates one entry per row, keyed by groupBy column name
 * @property {Array<Record<string, any>>} values one entry per row, keyed by measure name
 */

/**
 * @typedef {Object} SearchHit
 * @property {number} row index into the materialized view
 * @property {string} column column id (groupBy or measure name)
 * @property {any} value raw value as carried by the view
 * @property {string} formatted formatter-output (same string the user sees in the grid cell); falls back to String(value)
 * @property {"coordinate"|"measure"} kind which side of the row the hit came from — lets the UI label it appropriately
 */

const DEFAULT_LIMIT = 200;

/**
 * Search a materialized view for substring matches against the user's query.
 *
 * @param {{
 *   view: TabularViewSlice,
 *   coordinateColumns?: string[],
 *   measureColumns?: string[],
 *   formatCell?: ((row: number, columnId: string) => string | null) | null,
 *   limit?: number
 * }} ctx
 *   `view` is required; `coordinateColumns` / `measureColumns` constrain the search to specific
 *   columns (when omitted, every key found on the first non-empty row is searched);
 *   `formatCell` is invoked per `(row, columnId)` to obtain the displayed text — pass null to
 *   match the raw value only.
 * @param {string} query the needle the user typed
 * @returns {SearchHit[]} hits in row-major order, capped at `limit` (default 200) so a runaway
 *   match on a million-row view doesn't freeze the UI
 */
export function searchTabularView(ctx, query) {
	const needle = String(query || "")
		.toLowerCase()
		.trim();
	if (!needle) return [];
	const view = ctx && ctx.view;
	if (!view) return [];
	const coords = view.coordinates || [];
	const values = view.values || [];
	const limit = typeof ctx.limit === "number" && ctx.limit > 0 ? ctx.limit : DEFAULT_LIMIT;

	// Resolve which columns to scan. Explicit lists win; otherwise look at the first non-empty
	// row's keys. We sample from the first row only to keep the cost O(rowCount * colCount); a
	// sparse cell on a later row whose column is missing from row 0 will just not be searched —
	// acceptable for v1.
	const coordinateColumns = ctx.coordinateColumns || (coords[0] ? Object.keys(coords[0]) : []);
	const measureColumns = ctx.measureColumns || (values[0] ? Object.keys(values[0]) : []);
	const formatCell = ctx.formatCell || null;

	const rowCount = Math.max(coords.length, values.length);
	/** @type {SearchHit[]} */
	const hits = [];
	for (let row = 0; row < rowCount; row++) {
		const coordRow = coords[row] || {};
		const valueRow = values[row] || {};
		for (const col of coordinateColumns) {
			if (matchCell(coordRow[col], formatCell, row, col, needle, hits, "coordinate")) {
				if (hits.length >= limit) return hits;
			}
		}
		for (const col of measureColumns) {
			if (matchCell(valueRow[col], formatCell, row, col, needle, hits, "measure")) {
				if (hits.length >= limit) return hits;
			}
		}
	}
	return hits;
}

/**
 * Check whether a single cell matches the needle, pushing a hit onto the accumulator when it
 * does. Returns true iff a hit was pushed (so the caller can short-circuit on the limit).
 *
 * Numeric values match against BOTH the raw `String(n)` (so digit sequences inside larger
 * numbers find them — typing `234` matches a cell whose raw value is `71234`) AND the
 * formatter's output (so the user's mental model of "what the cell looks like" — e.g.
 * `71,234.00` — also matches).
 *
 * @param {any} raw
 * @param {((row: number, columnId: string) => string | null) | null} formatCell
 * @param {number} row
 * @param {string} col
 * @param {string} needle
 * @param {SearchHit[]} hits
 * @param {"coordinate"|"measure"} kind
 * @returns {boolean}
 */
function matchCell(raw, formatCell, row, col, needle, hits, kind) {
	if (raw === null || raw === undefined) return false;
	const rawStr = String(raw);
	const rawLower = rawStr.toLowerCase();
	let formatted = rawStr;
	if (formatCell) {
		try {
			const fromFormatter = formatCell(row, col);
			if (typeof fromFormatter === "string" && fromFormatter.length > 0) {
				formatted = fromFormatter;
			}
		} catch (e) {
			// Formatter threw — fall back to the raw string. The search is best-effort; one
			// bad column shouldn't take down the whole results list.
		}
	}
	const formattedLower = formatted.toLowerCase();
	if (rawLower.includes(needle) || formattedLower.includes(needle)) {
		hits.push({ row, column: col, value: raw, formatted, kind });
		return true;
	}
	return false;
}

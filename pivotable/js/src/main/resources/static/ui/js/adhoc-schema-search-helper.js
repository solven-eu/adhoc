// @ts-check

// Substring lookup over a cube's column and measure NAMES (not values — that's
// `searchTabularView` and `searchCoordinatesAcrossColumns`). Powers the navbar search's
// "Add to query" section: typing the start of a column or measure name surfaces those
// candidates so the user can extend their query without scrolling the wizard.
//
// Same case-insensitive substring contract as the other search helpers. Hits carry the
// `kind` so the navbar dropdown can label them ("Column" / "Measure") and route the click
// to the right side of the queryModel (`selectedColumns` vs `selectedMeasures`).

/**
 * @typedef {Object} SchemaHit
 * @property {"column"|"measure"} kind
 * @property {string} name column or measure name
 * @property {boolean} alreadyInQuery true when the queryModel already has this column/measure selected
 *   — caller uses it to render a muted "already added" hint instead of showing the same item twice
 */

const DEFAULT_LIMIT = 30;

/**
 * @param {{
 *   columns?: Record<string, any>,
 *   measures?: Record<string, any>,
 *   query: string,
 *   selectedColumns?: Record<string, boolean>,
 *   selectedMeasures?: Record<string, boolean>,
 *   limit?: number,
 * }} ctx
 *   `columns` is `cube.columns.columns` (keyed by column name); `measures` is `cube.measures`
 *   (keyed by measure name). Both are optional — when omitted that kind is simply not searched.
 * @returns {SchemaHit[]} hits sorted by kind (columns first, then measures), then alphabetical,
 *   capped at `limit` (default 30).
 */
export function searchCubeSchema(ctx) {
	const needle = String(ctx.query || "")
		.toLowerCase()
		.trim();
	if (!needle) return [];
	const limit = typeof ctx.limit === "number" && ctx.limit > 0 ? ctx.limit : DEFAULT_LIMIT;
	const selectedColumns = ctx.selectedColumns || {};
	const selectedMeasures = ctx.selectedMeasures || {};
	/** @type {SchemaHit[]} */
	const hits = [];

	const columns = ctx.columns || {};
	for (const name of Object.keys(columns)) {
		if (name.toLowerCase().includes(needle)) {
			hits.push({ kind: "column", name, alreadyInQuery: selectedColumns[name] === true });
		}
	}
	const measures = ctx.measures || {};
	for (const name of Object.keys(measures)) {
		if (name.toLowerCase().includes(needle)) {
			hits.push({ kind: "measure", name, alreadyInQuery: selectedMeasures[name] === true });
		}
	}

	hits.sort((a, b) => {
		if (a.kind !== b.kind) return a.kind < b.kind ? -1 : 1; // "column" < "measure" alphabetically
		return a.name < b.name ? -1 : a.name > b.name ? 1 : 0;
	});

	return hits.slice(0, limit);
}

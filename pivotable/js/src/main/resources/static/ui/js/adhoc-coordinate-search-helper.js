// @ts-check

// Phase B1 of the navbar search: a substring lookup across the per-column coordinate samples
// the SPA has ALREADY learned during the session — every time the user opens a column-filter
// dropdown or hits the bulk-coordinates endpoint, the adhoc store grows
// `state.columns[<endpoint>-<cube>-<column>] = { coordinates: [...] }`.
//
// This helper consults that cache (no backend round-trip) and returns the `{column,
// coordinate}` pairs whose coordinate string matches the user's query. Cheap and instant —
// the perfect first answer to "which column has a value matching <X>?" before the user opts
// into the (much heavier) Phase B2 backend fan-out across every column.
//
// Matching is the SAME substring contract as `searchTabularView`: case-insensitive, raw
// String form. The user is typing a value they remember, not a regex.

/**
 * @typedef {Object} CoordinateHit
 * @property {string} column column name
 * @property {string|number} coordinate one of the cached coordinate values for that column
 * @property {string} formatted same as `String(coordinate)` for now — kept as a separate field so
 *   future formatter-aware variants can override (e.g. show a localised date) without breaking callers
 */

const DEFAULT_LIMIT = 50;

/**
 * Walk the per-column coordinate cache and return substring-matching hits.
 *
 * @param {{
 *   columns: Record<string, { coordinates?: Array<string|number>, column?: string }>,
 *   endpointId: string,
 *   cubeId: string,
 *   query: string,
 *   limit?: number,
 *   excludeColumns?: string[],
 * }} ctx
 *   `columns` is the adhoc store's `state.columns` record (or any object with the same shape).
 *   `endpointId` + `cubeId` scope the scan (cache keys are `${endpointId}-${cubeId}-${column}`).
 *   `excludeColumns` lets the caller hide columns already in the active groupBy — those would
 *   produce a no-op click. Comparison is exact-string on the column name.
 * @returns {CoordinateHit[]} hits sorted (a) by column name alphabetical, (b) coordinate alphabetical.
 *   The list is capped at `limit` (default 50). Caller may slice further for rendering.
 */
export function searchCoordinatesAcrossColumns(ctx) {
	const needle = String(ctx.query || "")
		.toLowerCase()
		.trim();
	if (!needle) return [];
	const columns = ctx.columns || {};
	const endpointId = String(ctx.endpointId || "");
	const cubeId = String(ctx.cubeId || "");
	if (!endpointId || !cubeId) return [];
	const limit = typeof ctx.limit === "number" && ctx.limit > 0 ? ctx.limit : DEFAULT_LIMIT;
	const exclude = new Set((ctx.excludeColumns || []).map(String));

	const prefix = endpointId + "-" + cubeId + "-";
	/** @type {CoordinateHit[]} */
	const hits = [];
	for (const [key, entry] of Object.entries(columns)) {
		if (!key.startsWith(prefix)) continue;
		// Prefer the entry's own `column` field if the server attached it; fall back to the
		// suffix of the cache key. The server-side ColumnStatistics carries `column`, but
		// individual single-column fetches may not — be defensive.
		const columnName = entry && entry.column ? String(entry.column) : key.slice(prefix.length);
		if (exclude.has(columnName)) continue;
		const coords = entry && entry.coordinates;
		if (!Array.isArray(coords)) continue;
		for (const c of coords) {
			if (c === null || c === undefined) continue;
			const asStr = String(c);
			if (asStr.toLowerCase().includes(needle)) {
				hits.push({ column: columnName, coordinate: c, formatted: asStr });
				if (hits.length >= limit) {
					// Don't sort+cap pessimistically — once we have `limit` hits, return early
					// to keep the worst case bounded on a cube with many huge cardinality
					// columns. The early-exit set may not be the alphabetically-first hits,
					// but it's deterministic per the column-iteration order, which matches
					// the user's session-discovery order anyway.
					return hits.sort(sortHits);
				}
			}
		}
	}
	return hits.sort(sortHits);
}

function sortHits(a, b) {
	if (a.column !== b.column) return a.column < b.column ? -1 : 1;
	return a.formatted < b.formatted ? -1 : a.formatted > b.formatted ? 1 : 0;
}

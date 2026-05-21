// @ts-check
//
// Pure helper: convert a persisted query-history snapshot into a Mermaid `graph TD` source.
// Extracted from `adhoc-query-history-modal.js` so the conversion can be Vitest-tested without
// pulling mermaid (DOM-bound) into the spec.
//
// Output shape matches what the existing query-plan modal already renders — same TD/LR direction
// flag, same `classDef` highlight convention — so the visual idiom carries over.

import { diffLabel } from "./adhoc-query-history-store.js";

/** Color used to highlight the user's current node. Matches the "running" tone in the plan modal. */
const CURRENT_FILL = "#a6e3f5";
const CURRENT_STROKE = "#1e6f8e";

/**
 * Escape a content-hash into a Mermaid-safe node id. Hashes are hex (cyrb53 output) so they're
 * already alnum, but mermaid is picky about leading characters — prefix with `n_` to be safe.
 *
 * @param {string} hash
 * @returns {string}
 */
function nodeId(hash) {
	return "n_" + String(hash).replace(/[^a-zA-Z0-9_]/g, "_");
}

/**
 * Escape user-supplied text for embedding inside a quoted Mermaid label. Quotes are the only
 * character that can derail the parser inside double-quoted strings; the rest passes through
 * (Mermaid handles `+`, `−`, `,`, whitespace, etc. natively).
 *
 * @param {string} s
 * @returns {string}
 */
function escapeLabel(s) {
	return String(s).replace(/["\\]/g, "\\$&");
}

/**
 * Build a node label: one row per entity (groupBy column, filter column, measure), with the visit
 * count rendered as a styled chip rather than appended plain-text. Relies on
 * {@code securityLevel: "loose"} so mermaid passes through {@code <br/>} and inline
 * {@code <span class=...>} markup — same pattern as
 * {@code adhoc-query-plan-mermaid.js#formatNodeStatsSnippet}. Companion CSS is loaded once in
 * {@code static/index.html} ({@code .mermaid .adhoc-history-*} block).
 *
 * <p>Each entity prefix:
 * <ul>
 *   <li>{@code #col} — a groupBy column</li>
 *   <li>{@code ⊕col} — a filter-referenced column (distinct glyph so it reads as "constrained by"
 *       rather than "grouped by")</li>
 *   <li>{@code Σmeasure} — an aggregated measure</li>
 * </ul>
 * Overflow past {@code maxItems} is shown as a single "+N more" row, NOT silently truncated, so
 * the user can see how much they're not seeing at a glance.
 *
 * @param {any} node
 * @param {number} [maxItems]
 * @returns {string}
 */
function nodeLabel(node, maxItems = 6) {
	const cols = node.columnNames || [];
	const filterCols = (node.filterColumnNames || []).filter((c) => !cols.includes(c));
	const meas = node.measureNames || [];

	// Build one row per entity. The structured rows give the user a quick "what's in this query"
	// scan that a comma-separated string cannot — particularly for queries with 6+ groupBy
	// columns where the eye loses the boundaries.
	/** @type {string[]} */
	const rows = [];
	const remaining = () => Math.max(0, maxItems - rows.length);

	for (const c of cols) {
		if (rows.length >= maxItems) break;
		rows.push(`<span class="adhoc-history-entity adhoc-history-entity--col">#${escapeLabel(c)}</span>`);
	}
	for (const c of filterCols) {
		if (rows.length >= maxItems) break;
		rows.push(`<span class="adhoc-history-entity adhoc-history-entity--filter">⊕${escapeLabel(c)}</span>`);
	}
	for (const m of meas) {
		if (rows.length >= maxItems) break;
		rows.push(`<span class="adhoc-history-entity adhoc-history-entity--measure">Σ${escapeLabel(m)}</span>`);
	}

	const totalEntities = cols.length + filterCols.length + meas.length;
	const overflow = totalEntities - rows.length;
	if (overflow > 0) {
		rows.push(`<span class="adhoc-history-entity adhoc-history-entity--more">+${overflow} more</span>`);
	}
	if (rows.length === 0) {
		rows.push(`<span class="adhoc-history-entity adhoc-history-entity--empty">∅ empty query</span>`);
	}

	// Visit-count chip — anchored to the top-right corner of the label region via a wrapping
	// `position: relative` div + the chip's own `position: absolute` (defined in CSS). Only
	// shown when visitCount > 1; a single-visit node would carry a "×1" badge that mostly adds
	// noise.
	const visitCount = node.visitCount || 1;
	const chip = visitCount > 1 ? `<span class="adhoc-history-chip adhoc-history-chip--visits">×${visitCount}</span>` : "";

	void remaining;
	// Wrap entity rows + the absolutely-positioned chip in a single label container. The chip
	// is emitted FIRST in source order so it falls into the top of the absolute layer (right:0)
	// regardless of how many entity rows follow. The wrapper carries right-padding (in CSS) so
	// the chip doesn't sit on top of the longest entity row.
	return `<div class="adhoc-history-label">${chip}${rows.join("<br/>")}</div>`;
}

/**
 * Convert a history-store snapshot into a Mermaid `graph TD` (or `graph LR`) source string. The
 * caller (the modal) feeds the result to `mermaid.render(id, source)`.
 *
 * <p>Returns a placeholder source ("empty graph") when the snapshot has no nodes — mermaid
 * doesn't handle empty input gracefully and the modal would render a confusing blank canvas.
 *
 * @param {{ nodes: Record<string, any>, edges: Record<string, Record<string, any>> } | null | undefined} snapshot
 * @param {{ currentHash?: string | null, direction?: "TD" | "LR" }} [opts]
 * @returns {string}
 */
export function snapshotToMermaid(snapshot, opts = {}) {
	const direction = opts.direction ?? "TD";
	const currentHash = opts.currentHash ?? null;
	const lines = [`graph ${direction}`];

	if (!snapshot || !snapshot.nodes || Object.keys(snapshot.nodes).length === 0) {
		// Mermaid refuses an empty graph; emit a single placeholder so the modal renders
		// something the user can read rather than a parse error.
		lines.push(`  empty["No history yet — run a query to start building your graph."]`);
		return lines.join("\n");
	}

	// Sort nodes deterministically so the rendered SVG is stable across opens (helpful in
	// screenshots/diffs and for the user's mental map). Order: most recent first.
	const sortedNodes = Object.values(snapshot.nodes).sort((a, b) => {
		const ta = Date.parse(a.lastSeenAt || "") || 0;
		const tb = Date.parse(b.lastSeenAt || "") || 0;
		return tb - ta;
	});

	for (const node of sortedNodes) {
		const id = nodeId(node.id);
		// `nodeLabel` returns HTML markup intentionally — `securityLevel: "loose"` (set in the
		// modal's mermaid.initialize call) instructs mermaid to pass it through. The individual
		// entity names INSIDE the markup were already escaped by `nodeLabel`; the structural
		// `<span>` / `<br/>` wrappers we produce ourselves are not user-controlled and don't need
		// escaping. We use `[` brackets (not the `(` rounded variant) so multi-line content
		// renders with mermaid's default rectangular shape.
		lines.push(`  ${id}["${nodeLabel(node)}"]`);
		// Click handler attaches in the modal via mermaid's click binding — we just mark which
		// hash this node carries so the modal's onclick can look it up.
		lines.push(`  click ${id} call onHistoryNodeClick("${node.id}")`);
		if (node.id === currentHash) {
			lines.push(`  class ${id} historyCurrent`);
		}
	}

	for (const fromHash of Object.keys(snapshot.edges || {})) {
		const outs = snapshot.edges[fromHash];
		for (const toHash of Object.keys(outs)) {
			const edge = outs[toHash];
			const label = escapeLabel(diffLabel(edge?.diff) || "·");
			lines.push(`  ${nodeId(fromHash)} -->|"${label}"| ${nodeId(toHash)}`);
		}
	}

	lines.push(`  classDef historyCurrent fill:${CURRENT_FILL},stroke:${CURRENT_STROKE},stroke-width:2px`);

	return lines.join("\n");
}

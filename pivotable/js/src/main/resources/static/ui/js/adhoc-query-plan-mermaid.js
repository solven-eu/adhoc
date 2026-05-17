// @ts-check

// Pure helper: convert a /plan/snapshot JSON payload into a Mermaid `graph TD` source string.
// Extracted from the modal component so the conversion can be Vitest-tested without DOM / mermaid.
//
// Node ids are assigned deterministically as `n0`, `n1`, … in DFS-discovery order. Shared children
// (DAG case where two parents point at the same node) are deduplicated by `subject` so the rendered
// graph stays a DAG instead of exploding into a tree.

/**
 * Per-state color palette used BOTH by the Mermaid `classDef` lines below AND by the legend
 * rendered in `adhoc-query-plan-mermaid-modal.js`. Centralising the palette here avoids drift —
 * if you change a colour, both the diagram and the legend follow.
 *
 * <p>The `label` field is the human-readable name surfaced in the legend (the underlying
 * `NodeState` enum values are upper-case engine-internal names).
 *
 * @type {Record<'done'|'running'|'pending'|'failed', { fill: string, stroke: string, label: string }>}
 */
export const STATE_PALETTE = {
	done: { fill: "#a8d8a8", stroke: "#347634", label: "Done" },
	running: { fill: "#a6e3f5", stroke: "#1e6f8e", label: "Running" },
	pending: { fill: "#e0e0e0", stroke: "#888", label: "Pending / Skipped" },
	failed: { fill: "#f5a6a6", stroke: "#8e1e1e", label: "Failed" },
};

/** @typedef {{elapsedMs?: number, rowsIn?: number|null, rowsOut?: number|null, estimated?: boolean}} PlanNodeStats */
/** @typedef {{id: string, state: string, label: string, subject?: any, operator?: string, details?: Record<string,string>, stats?: PlanNodeStats}} PlanNode */
/** @typedef {{parentId: string, childId: string}} PlanEdge */
/** @typedef {{rootId: string, nodes: PlanNode[], edges: PlanEdge[]}} PlanSnapshot */

/**
 * Render a row count as a compact human string: `null`/`undefined` → `""`, `< 1_000` → unchanged,
 * `< 1_000_000` → "1.2k", larger → "3.4M" / "1.2B". Keeps one decimal so the order of magnitude is
 * preserved without exploding the node width. The fallback for negative / NaN values is `""` —
 * those would only be sent by a buggy projector and we'd rather render nothing than something
 * confusing.
 *
 * @param {number|null|undefined} count
 * @returns {string}
 */
export function formatRowCount(count) {
	if (count == null || !Number.isFinite(count) || count < 0) return "";
	if (count < 1000) return String(count);
	if (count < 1_000_000) return (count / 1000).toFixed(1).replace(/\.0$/, "") + "k";
	if (count < 1_000_000_000) return (count / 1_000_000).toFixed(1).replace(/\.0$/, "") + "M";
	return (count / 1_000_000_000).toFixed(1).replace(/\.0$/, "") + "B";
}

/**
 * Render a millisecond duration as a compact human string. The thresholds mirror what the engine's
 * `PepperLogHelper.humanDuration` produces server-side, so the SPA Mermaid label and the
 * `DagExplainerForPerfs` SLF4J log line read alike. Returns `""` for `null`/`undefined`/negative.
 *
 * @param {number|null|undefined} ms
 * @returns {string}
 */
export function formatDuration(ms) {
	if (ms == null || !Number.isFinite(ms) || ms < 0) return "";
	if (ms < 1) return "<1ms";
	if (ms < 1000) return Math.round(ms) + "ms";
	if (ms < 60_000) return (ms / 1000).toFixed(1).replace(/\.0$/, "") + "s";
	const totalSeconds = Math.round(ms / 1000);
	const minutes = Math.floor(totalSeconds / 60);
	const seconds = totalSeconds % 60;
	return seconds === 0 ? `${minutes}m` : `${minutes}m ${seconds}s`;
}

/**
 * Build the per-node stats suffix appended to the Mermaid label (after a `<br/>`). Returned empty
 * when no stat is known, so empty plans / PENDING nodes don't grow a dangling line break. The
 * formatting bubbles up information from `DagExplainerForPerfs#additionalInfo` — duration,
 * rowsOut (the projector's "size") and rowsIn when present — separated by middle dots.
 *
 * @param {PlanNodeStats|undefined} stats
 * @returns {string} either `""` or `<br/><small>…</small>` ready to splice into the Mermaid label
 */
export function formatNodeStatsSnippet(stats) {
	if (!stats) return "";
	// One chip per metric so each is visually distinct (per user request). Bootstrap-Icons glyphs
	// stand in for the labels — the font is already loaded by `index.html`, and Mermaid renders
	// the `<i class="bi …">` markup verbatim inside the foreignObject-rendered HTML label.
	//  - bi-stopwatch → elapsed time
	//  - bi-stack → rows produced (rowsOut == projector's "size")
	//  - bi-box-arrow-in-right → rows consumed (rowsIn — rarely populated today but surfaced
	//    distinctly so future projectors can light it up without ambiguity)
	// Chip styling — including the inter-chip gap — lives in `index.html` under
	// `.mermaid .adhoc-perf-chip`.
	const chips = [];
	const duration = formatDuration(stats.elapsedMs);
	if (duration) chips.push(`<span class="adhoc-perf-chip adhoc-perf-chip--time"><i class="bi bi-stopwatch"></i> ${duration}</span>`);
	const rowsOut = formatRowCount(stats.rowsOut);
	if (rowsOut) chips.push(`<span class="adhoc-perf-chip adhoc-perf-chip--rows"><i class="bi bi-stack"></i> ${rowsOut}</span>`);
	const rowsIn = formatRowCount(stats.rowsIn);
	if (rowsIn) chips.push(`<span class="adhoc-perf-chip adhoc-perf-chip--rows-in"><i class="bi bi-box-arrow-in-right"></i> ${rowsIn}</span>`);
	if (chips.length === 0) return "";
	return `<br/>${chips.join("")}`;
}

/**
 * Build the per-property detail suffix appended to the Mermaid label (after a `<br/>`). The
 * backend populates `node.details` with a key→stringified-value map (see
 * `ACubeQueryStep#toDetails` and `CubeQueryEngine#cubeQueryDetails` on the server side). We
 * render one line per entry, in iteration order, so the SPA's multi-line label mirrors the
 * structure a `DagExplainer`-style ASCII renderer would produce.
 *
 * <p>The `sql` key is intentionally skipped: SQL leaves already pick a distinct cylinder shape
 * via `shapeFor`, and the dedicated SQL list rendered next to the diagram (in
 * `adhoc-query-plan-mermaid-modal.js`) shows the full query body — duplicating it inside the
 * node label would just clutter the graph.
 *
 * <p>Returns `""` when `details` is empty, null, or contains only filtered-out keys — so an
 * empty `details` map doesn't introduce a dangling `<br/>` next to the headline.
 *
 * @param {Record<string,string>|undefined|null} details
 * @returns {string} either `""` or a `<br/>key=value<br/>key=value…` snippet ready to splice in
 */
export function formatDetailsSnippet(details) {
	if (!details) return "";
	const parts = [];
	for (const [key, value] of Object.entries(details)) {
		// Skip the sql payload — handled separately via the cylinder shape + dedicated list panel.
		if (key === "sql") continue;
		if (value == null || value === "") continue;
		// Per-value escapeLabel call so any `"` / `[` / `]` / `|` in the backend-supplied value
		// can't break Mermaid's label parsing; the wrapping `<br/>` / key= markup we add is fully
		// controlled by us and isn't escaped.
		parts.push(`${escapeLabel(key)}=${escapeLabel(value)}`);
	}
	if (parts.length === 0) return "";
	return "<br/>" + parts.join("<br/>");
}

/**
 * Defang a label so Mermaid's parser doesn't choke. Mermaid uses `[`...`]` to delimit node labels
 * and a raw `"` breaks them; we flip brackets to parens and HTML-escape quotes.
 *
 * @param {string} raw
 * @returns {string}
 */
function escapeLabel(raw) {
	if (raw == null) return "";
	return String(raw).replace(/"/g, "&quot;").replace(/\[/g, "(").replace(/\]/g, ")").replace(/\|/g, "/");
}

/**
 * Map an operator + details to (a) the Mermaid bracket shape and (b) the role-style classDef name.
 *
 * <p>Shapes encode the role at a glance:
 * <ul>
 *   <li>cube steps → rectangle `[label]` (default, the "logic" of the query)</li>
 *   <li>table steps → stadium `([label])` (transition to the table layer)</li>
 *   <li>merged table queries → hexagon `{{label}}` (the merger output that hits the DB)</li>
 *   <li>SQL / native-query leaves → cylinder `[(label)]` (the database side, the row-storage symbol)</li>
 *   <li>fall-back / unknown → rectangle</li>
 * </ul>
 *
 * <p>Returns also the role bucket key the caller files into for classDef styling. The `state` bucket
 * remains independent so a node gets both a role tint and a state border.
 *
 * @param {string|undefined} operator
 * @param {Record<string,string>|undefined} details
 * @returns {{ open: string, close: string, role: 'cubeQuery'|'cube'|'table'|'tableQuery'|'sql'|'other' }}
 */
function shapeFor(operator, details) {
	if (details && details.sql) {
		return { open: "[(", close: ")]", role: "sql" };
	}
	switch (operator) {
		case "CUBE_QUERY":
			// Trapezoid-ish (subroutine shape) signals "this is the user-facing query, not an internal step".
			// Distinct from CUBE_STEP so the root reads as a heading regardless of how many roots the planner produced.
			return { open: "[[", close: "]]", role: "cubeQuery" };
		case "CUBE_STEP":
		case "MEASURE_REF":
			return { open: "[", close: "]", role: "cube" };
		case "TABLE_STEP":
			return { open: "([", close: "])", role: "table" };
		case "TABLE_QUERY":
			return { open: "{{", close: "}}", role: "tableQuery" };
		case "COMPOSITE_FANOUT":
		case "SUB_CUBE_DELEGATION":
		case "MERGE":
			return { open: "((", close: "))", role: "other" };
		default:
			return { open: "[", close: "]", role: "other" };
	}
}

/**
 * Convert a plan snapshot to a Mermaid graph source string.
 *
 * <p>The direction parameter controls Mermaid's flowchart orientation:
 * <ul>
 *   <li>`"TD"` (default) — top-down, the historical layout. Reads root → leaves vertically.</li>
 *   <li>`"LR"` — left-to-right. More readable when the plan is wider than it is deep (typical of
 *     queries with many SQL leaves under a single CubeQueryStep), because Mermaid stops shrinking
 *     individual node widths to fit a fixed canvas width.</li>
 * </ul>
 * Any other value silently falls back to `"TD"` so a corrupted preference never produces invalid
 * Mermaid source.
 *
 * @param {PlanSnapshot} plan
 * @param {"TD"|"LR"} [direction="TD"] flowchart direction; ignored for the empty-plan early return
 * @returns {string} Mermaid `graph <direction>` source
 */
export function planToMermaid(plan, direction = "TD") {
	const header = direction === "LR" ? "graph LR" : "graph TD";
	if (!plan || !plan.nodes || plan.nodes.length === 0) return `${header}\n`;

	const lines = [header];
	/** @type {{done: string[], running: string[], pending: string[], failed: string[]}} */
	const stateBuckets = { done: [], running: [], pending: [], failed: [] };
	/** @type {Record<'cubeQuery'|'cube'|'table'|'tableQuery'|'sql'|'other', string[]>} */
	const roleBuckets = { cubeQuery: [], cube: [], table: [], tableQuery: [], sql: [], other: [] };
	// Per-role node-declaration strings. Collected during the iteration and emitted later, either at the
	// graph's top level (CUBE_QUERY root + uncategorised "other" nodes) or inside one of three
	// `subgraph … end` blocks that group the engine's three execution stages:
	//   - Cube query steps (CUBE_STEP + MEASURE_REF roles)
	//   - Table query steps (TABLE_STEP role — published per induced TableQueryStep)
	//   - Table queries (TABLE_QUERY V4 + SQL leaves)
	// Building these as separate lists is necessary because Mermaid expects all node declarations
	// for a subgraph to sit between its `subgraph` / `end` markers; we therefore can't emit them
	// in the projector's node-list order without splitting.
	/** @type {Record<'cubeQuery'|'cube'|'table'|'tableQuery'|'sql'|'other', string[]>} */
	const declarationsByRole = { cubeQuery: [], cube: [], table: [], tableQuery: [], sql: [], other: [] };

	// One pass over the deduplicated nodes list — the projector has already done the DAG dedup, so this is
	// a flat iteration, no recursion. Use the server-assigned node ids verbatim; the SPA does no remapping.
	for (const node of plan.nodes) {
		const shape = shapeFor(node.operator, node.details);
		// The stats snippet and per-property detail lines are NOT pushed through `escapeLabel` because
		// we want their `<br/>` markup to be preserved as-is — Mermaid renders them inline when
		// initialised with `securityLevel: "loose"`. The snippets are fully controlled by us (no
		// user-supplied text reaches them outside the per-property values, which we escapeLabel
		// individually), so HTML-escaping the wrapping markup is unnecessary.
		const detailsSnippet = formatDetailsSnippet(node.details);
		const statsSnippet = formatNodeStatsSnippet(node.stats);
		declarationsByRole[shape.role].push(`    ${node.id}${shape.open}"${escapeLabel(node.label)}${detailsSnippet}${statsSnippet}"${shape.close}`);
		assignStateBucket(stateBuckets, node.id, node.state);
		roleBuckets[shape.role].push(node.id);
	}

	// Edges are already deduplicated on the server: one entry per parent→child link. Mermaid handles
	// cross-subgraph edges natively, so we can emit them flat regardless of which subgraph the endpoints live in.
	const edges = (plan.edges || []).map((e) => `    ${e.parentId} --> ${e.childId}`);

	// Top-level declarations: the CUBE_QUERY root sits above everything as the heading, and the
	// generic "other" bucket (composite-cube fanout, sub-cube delegation, merge — none of which
	// belong to the cube/table/tableQuery trio) stays at the top level too rather than being
	// shoe-horned into one of the three stage subgraphs.
	lines.push(...declarationsByRole.cubeQuery);
	lines.push(...declarationsByRole.other);

	// The three engine-stage subgraphs. Emit only the non-empty ones so a graph that — say —
	// doesn't have any cube-side intermediate steps doesn't get an empty "Cube query steps" box.
	// Subgraph titles are wrapped in quotes so the spaces don't trip Mermaid's parser.
	if (declarationsByRole.cube.length > 0) {
		lines.push('    subgraph subCubeSteps ["Cube query steps"]');
		lines.push(...declarationsByRole.cube);
		lines.push("    end");
	}
	if (declarationsByRole.table.length > 0) {
		lines.push('    subgraph subTableSteps ["Table query steps"]');
		lines.push(...declarationsByRole.table);
		lines.push("    end");
	}
	// Merge TABLE_QUERY V4 nodes with their SQL-leaf children into one "Table queries" subgraph —
	// the user thinks of them as the same engine stage (the work that physically hits the DB).
	if (declarationsByRole.tableQuery.length + declarationsByRole.sql.length > 0) {
		lines.push('    subgraph subTableQueries ["Table queries"]');
		lines.push(...declarationsByRole.tableQuery);
		lines.push(...declarationsByRole.sql);
		lines.push("    end");
	}

	lines.push(...edges);
	// State-driven classDefs — colour fills. The palette is centralised in `STATE_PALETTE` so the
	// legend rendered next to the diagram (in the modal) stays in sync with what Mermaid paints.
	for (const stateKey of /** @type {const} */ (["done", "running", "pending", "failed"])) {
		const p = STATE_PALETTE[stateKey];
		lines.push(`    classDef ${stateKey} fill:${p.fill},stroke:${p.stroke},color:#000`);
	}
	// Role-driven classDefs — only stroke width/style. Applied AFTER state so the state's fill wins and the
	// role's stroke decoration accumulates. A dashed stroke distinguishes the "logic" cube nodes from the
	// "execution" table/SQL nodes which carry a thicker solid stroke.
	lines.push("    classDef roleCubeQuery stroke-width:3px,font-weight:bold");
	lines.push("    classDef roleCube stroke-dasharray:4 2");
	lines.push("    classDef roleTable stroke-width:2px");
	lines.push("    classDef roleTableQuery stroke-width:3px");
	lines.push("    classDef roleSql stroke-width:3px,font-family:monospace");
	lines.push("    classDef roleOther stroke-dasharray:2 2");
	if (stateBuckets.done.length > 0) lines.push(`    class ${stateBuckets.done.join(",")} done`);
	if (stateBuckets.running.length > 0) lines.push(`    class ${stateBuckets.running.join(",")} running`);
	if (stateBuckets.pending.length > 0) lines.push(`    class ${stateBuckets.pending.join(",")} pending`);
	if (stateBuckets.failed.length > 0) lines.push(`    class ${stateBuckets.failed.join(",")} failed`);
	if (roleBuckets.cubeQuery.length > 0) lines.push(`    class ${roleBuckets.cubeQuery.join(",")} roleCubeQuery`);
	if (roleBuckets.cube.length > 0) lines.push(`    class ${roleBuckets.cube.join(",")} roleCube`);
	if (roleBuckets.table.length > 0) lines.push(`    class ${roleBuckets.table.join(",")} roleTable`);
	if (roleBuckets.tableQuery.length > 0) lines.push(`    class ${roleBuckets.tableQuery.join(",")} roleTableQuery`);
	if (roleBuckets.sql.length > 0) lines.push(`    class ${roleBuckets.sql.join(",")} roleSql`);
	if (roleBuckets.other.length > 0) lines.push(`    class ${roleBuckets.other.join(",")} roleOther`);
	return lines.join("\n");
}

/**
 * Bucket the node id into one of the four CSS classes. SKIPPED falls back to pending (visually inert).
 *
 * @param {{done: string[], running: string[], pending: string[], failed: string[]}} buckets
 * @param {string} id
 * @param {string} state
 */
function assignStateBucket(buckets, id, state) {
	switch (state) {
		case "DONE":
			buckets.done.push(id);
			break;
		case "RUNNING":
			buckets.running.push(id);
			break;
		case "FAILED":
			buckets.failed.push(id);
			break;
		case "PENDING":
		case "SKIPPED":
		default:
			buckets.pending.push(id);
			break;
	}
}

/**
 * Walk the plan tree and collect every node carrying a `details.sql` payload — the SQL leaves published by
 * `JooqTableWrapper`. Returned in document-order (DFS, parent before children) so the modal's "SQL queries
 * in this plan" list matches the visual order the Mermaid graph displays.
 *
 * <p>Deduplicates by SQL text so the same query rendered against multiple anchors (DAG-shared
 * `TableQueryV4` reached from several `TableQueryStep`s) shows up once. The dedup uses the raw SQL — two
 * different anchors but the same SQL = one row.
 *
 * @param {PlanSnapshot} plan
 * @returns {{ label: string, sql: string }[]}
 */
export function collectSqlLeaves(plan) {
	/** @type {{label: string, sql: string}[]} */
	const out = [];
	const seen = new Set();
	if (!plan || !plan.nodes) return out;

	// Flat sweep — the projector's nodes list is in DFS-discovery order, so iterating preserves the document
	// order the Mermaid graph displays. Dedup by SQL text is defensive; the projector already dedupes by
	// subject so any two SQL leaves with the same body share one node.
	for (const node of plan.nodes) {
		const details = /** @type {Record<string,string>|undefined} */ (node.details);
		if (details && details.sql && !seen.has(details.sql)) {
			seen.add(details.sql);
			out.push({ label: node.label || "sql", sql: details.sql });
		}
	}
	return out;
}

export default { planToMermaid, collectSqlLeaves };

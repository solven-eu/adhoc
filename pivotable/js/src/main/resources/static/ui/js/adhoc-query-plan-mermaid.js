// @ts-check

// Pure helper: convert a /plan/snapshot JSON payload into a Mermaid `graph TD` source string.
// Extracted from the modal component so the conversion can be Vitest-tested without DOM / mermaid.
//
// Node ids are assigned deterministically as `n0`, `n1`, … in DFS-discovery order. Shared children
// (DAG case where two parents point at the same node) are deduplicated by `subject` so the rendered
// graph stays a DAG instead of exploding into a tree.

/** @typedef {{state: string, label: string, subject?: any, operator?: string, details?: Record<string,string>, children?: PlanNode[]}} PlanNode */
/** @typedef {{root: PlanNode}} PlanSnapshot */

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
 * @returns {{ open: string, close: string, role: 'cube'|'table'|'tableQuery'|'sql'|'other' }}
 */
function shapeFor(operator, details) {
	if (details && details.sql) {
		return { open: "[(", close: ")]", role: "sql" };
	}
	switch (operator) {
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
 * @param {PlanSnapshot} plan
 * @returns {string} Mermaid `graph TD` source
 */
export function planToMermaid(plan) {
	if (!plan || !plan.root) return "graph TD\n";

	/** @type {Map<any, string>} */
	const subjectToId = new Map();
	/** @type {Set<any>} */
	const visited = new Set();

	const lines = ["graph TD"];
	const edges = [];
	/** @type {{done: string[], running: string[], pending: string[], failed: string[]}} */
	const stateBuckets = { done: [], running: [], pending: [], failed: [] };
	/** @type {Record<'cube'|'table'|'tableQuery'|'sql'|'other', string[]>} */
	const roleBuckets = { cube: [], table: [], tableQuery: [], sql: [], other: [] };

	/** @type {PlanNode[]} */
	const stack = [plan.root];
	while (stack.length > 0) {
		const node = /** @type {PlanNode} */ (stack.pop());
		// Subject can be anything (string, object, …). Fall back to the label when no explicit subject is
		// provided — the projector always sets one, but defensive code keeps the helper resilient to
		// hand-crafted test plans.
		const key = node.subject != null ? node.subject : node.label;
		if (visited.has(key)) continue;
		visited.add(key);

		let id = subjectToId.get(key);
		if (id === undefined) {
			id = `n${subjectToId.size}`;
			subjectToId.set(key, id);
		}
		const shape = shapeFor(node.operator, node.details);
		lines.push(`    ${id}${shape.open}"${escapeLabel(node.label)}"${shape.close}`);
		assignStateBucket(stateBuckets, id, node.state);
		roleBuckets[shape.role].push(id);

		if (node.children && node.children.length > 0) {
			for (const child of node.children) {
				const childKey = child.subject != null ? child.subject : child.label;
				let childId = subjectToId.get(childKey);
				if (childId === undefined) {
					childId = `n${subjectToId.size}`;
					subjectToId.set(childKey, childId);
				}
				edges.push(`    ${id} --> ${childId}`);
				stack.push(child);
			}
		}
	}

	lines.push(...edges);
	// State-driven classDefs — colour fills. Same palette as before so existing snapshots stay visually stable.
	lines.push("    classDef done fill:#a8d8a8,stroke:#347634,color:#000");
	lines.push("    classDef running fill:#a6e3f5,stroke:#1e6f8e,color:#000");
	lines.push("    classDef pending fill:#e0e0e0,stroke:#888,color:#000");
	lines.push("    classDef failed fill:#f5a6a6,stroke:#8e1e1e,color:#000");
	// Role-driven classDefs — only stroke width/style. Applied AFTER state so the state's fill wins and the
	// role's stroke decoration accumulates. A dashed stroke distinguishes the "logic" cube nodes from the
	// "execution" table/SQL nodes which carry a thicker solid stroke.
	lines.push("    classDef roleCube stroke-dasharray:4 2");
	lines.push("    classDef roleTable stroke-width:2px");
	lines.push("    classDef roleTableQuery stroke-width:3px");
	lines.push("    classDef roleSql stroke-width:3px,font-family:monospace");
	lines.push("    classDef roleOther stroke-dasharray:2 2");
	if (stateBuckets.done.length > 0) lines.push(`    class ${stateBuckets.done.join(",")} done`);
	if (stateBuckets.running.length > 0) lines.push(`    class ${stateBuckets.running.join(",")} running`);
	if (stateBuckets.pending.length > 0) lines.push(`    class ${stateBuckets.pending.join(",")} pending`);
	if (stateBuckets.failed.length > 0) lines.push(`    class ${stateBuckets.failed.join(",")} failed`);
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
	if (!plan || !plan.root) return out;

	/** @type {PlanNode[]} */
	const stack = [plan.root];
	while (stack.length > 0) {
		// pop is LIFO so we push children in reverse to preserve document order
		const node = /** @type {PlanNode} */ (stack.pop());
		const details = /** @type {Record<string,string>|undefined} */ (/** @type {any} */ (node).details);
		if (details && details.sql && !seen.has(details.sql)) {
			seen.add(details.sql);
			out.push({ label: node.label || "sql", sql: details.sql });
		}
		if (node.children && node.children.length > 0) {
			for (let i = node.children.length - 1; i >= 0; i--) {
				stack.push(node.children[i]);
			}
		}
	}
	return out;
}

export default { planToMermaid, collectSqlLeaves };

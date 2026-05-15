// @ts-check

// Pure helper: convert a /plan/snapshot JSON payload into a Mermaid `graph TD` source string.
// Extracted from the modal component so the conversion can be Vitest-tested without DOM / mermaid.
//
// Node ids are assigned deterministically as `n0`, `n1`, … in DFS-discovery order. Shared children
// (DAG case where two parents point at the same node) are deduplicated by `subject` so the rendered
// graph stays a DAG instead of exploding into a tree.

/** @typedef {{state: string, label: string, subject?: any, children?: PlanNode[]}} PlanNode */
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
	const buckets = { done: [], running: [], pending: [], failed: [] };

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
		lines.push(`    ${id}["${escapeLabel(node.label)}"]`);
		assignStateBucket(buckets, id, node.state);

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
	lines.push("    classDef done fill:#a8d8a8,stroke:#347634,color:#000");
	lines.push("    classDef running fill:#a6e3f5,stroke:#1e6f8e,color:#000");
	lines.push("    classDef pending fill:#e0e0e0,stroke:#888,color:#000");
	lines.push("    classDef failed fill:#f5a6a6,stroke:#8e1e1e,color:#000");
	if (buckets.done.length > 0) lines.push(`    class ${buckets.done.join(",")} done`);
	if (buckets.running.length > 0) lines.push(`    class ${buckets.running.join(",")} running`);
	if (buckets.pending.length > 0) lines.push(`    class ${buckets.pending.join(",")} pending`);
	if (buckets.failed.length > 0) lines.push(`    class ${buckets.failed.join(",")} failed`);
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

export default { planToMermaid };

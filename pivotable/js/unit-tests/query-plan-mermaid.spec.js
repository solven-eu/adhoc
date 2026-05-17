// @ts-check
import { expect, test } from "vitest";

import {
	planToMermaid,
	collectSqlLeaves,
	formatRowCount,
	formatDuration,
	formatNodeStatsSnippet,
	formatDetailsSnippet,
	STATE_PALETTE,
} from "@/js/adhoc-query-plan-mermaid.js";

// Tests for the pure plan → Mermaid converter. No DOM, no mermaid runtime — just string output we can inspect.
//
// The plan model is graph-shaped: `{ rootId, nodes: [...], edges: [{parentId, childId}, ...] }`. Most fixtures here
// describe the plan as a tree for readability and pass through `treeToGraph(...)` which lowers the tree into the
// graph form the renderer accepts. Tests that exercise DAG behaviour (multi-incoming edges on a shared node) build
// the graph directly so we can pin "one node, N edges" rather than "N copies of the same node".

/**
 * Lower a tree-shape descriptor (the shape we used pre-graph-refactor) into the graph form `planToMermaid` accepts.
 * Node ids are assigned in DFS-discovery order as `n0`, `n1`, … — same scheme the projector uses, so assertions
 * matching node ids stay stable.
 *
 * @param {any} rootTree
 * @returns {{rootId: string, nodes: any[], edges: {parentId: string, childId: string}[]}}
 */
function treeToGraph(rootTree) {
	const nodes = [];
	const edges = [];
	let nextId = 0;
	function visit(treeNode) {
		const id = "n" + nextId++;
		const { children, ...rest } = treeNode;
		nodes.push({ ...rest, id });
		for (const child of children || []) {
			edges.push({ parentId: id, childId: visit(child) });
		}
		return id;
	}
	const rootId = visit(rootTree);
	return { rootId, nodes, edges };
}

test("empty / missing plan emits a header-only graph (no nodes)", () => {
	expect(planToMermaid(/** @type {any} */ (null))).toBe("graph TD\n");
});

test("direction='LR' emits a graph LR header (and the empty-plan path honours it too)", () => {
	expect(planToMermaid(/** @type {any} */ (null), "LR")).toBe("graph LR\n");

	const plan = treeToGraph({ subject: "root", label: "the-root", state: "DONE" });
	const out = planToMermaid(plan, "LR");
	expect(out.split("\n")[0]).toBe("graph LR");
	expect(out).not.toContain("graph TD");
});

test("direction defaults to TD when omitted or set to an unknown value (defensive)", () => {
	const plan = treeToGraph({ subject: "root", label: "the-root", state: "DONE" });
	expect(planToMermaid(plan).split("\n")[0]).toBe("graph TD");
	expect(planToMermaid(plan, /** @type {any} */ ("BOGUS")).split("\n")[0]).toBe("graph TD");
});

test("formatRowCount: thresholds and absent values", () => {
	expect(formatRowCount(null)).toBe("");
	expect(formatRowCount(undefined)).toBe("");
	expect(formatRowCount(NaN)).toBe("");
	expect(formatRowCount(-1)).toBe("");
	expect(formatRowCount(0)).toBe("0");
	expect(formatRowCount(42)).toBe("42");
	expect(formatRowCount(999)).toBe("999");
	expect(formatRowCount(1000)).toBe("1k");
	expect(formatRowCount(1234)).toBe("1.2k");
	expect(formatRowCount(999_500)).toBe("999.5k");
	expect(formatRowCount(1_234_567)).toBe("1.2M");
	expect(formatRowCount(3_000_000_000)).toBe("3B");
});

test("formatDuration: thresholds and absent values", () => {
	expect(formatDuration(null)).toBe("");
	expect(formatDuration(undefined)).toBe("");
	expect(formatDuration(NaN)).toBe("");
	expect(formatDuration(-1)).toBe("");
	expect(formatDuration(0)).toBe("<1ms");
	expect(formatDuration(0.4)).toBe("<1ms");
	expect(formatDuration(7)).toBe("7ms");
	expect(formatDuration(123)).toBe("123ms");
	expect(formatDuration(999)).toBe("999ms");
	expect(formatDuration(1000)).toBe("1s");
	expect(formatDuration(1234)).toBe("1.2s");
	expect(formatDuration(59_999)).toBe("60s");
	expect(formatDuration(60_000)).toBe("1m");
	expect(formatDuration(125_000)).toBe("2m 5s");
});

test("formatNodeStatsSnippet: empty stats / all-null stats produce an empty string", () => {
	expect(formatNodeStatsSnippet(undefined)).toBe("");
	expect(formatNodeStatsSnippet({})).toBe("");
	expect(formatNodeStatsSnippet({ elapsedMs: null, rowsIn: null, rowsOut: null })).toBe("");
});

test("formatNodeStatsSnippet: one chip per metric — timing chip uses --time + stopwatch icon", () => {
	expect(formatNodeStatsSnippet({ elapsedMs: 42 })).toBe(
		'<br/><span class="adhoc-perf-chip adhoc-perf-chip--time"><i class="bi bi-stopwatch"></i> 42ms</span>',
	);
});

test("formatNodeStatsSnippet: timing + rowsOut produce two SEPARATE chips concatenated (no middle-dot)", () => {
	expect(formatNodeStatsSnippet({ elapsedMs: 12, rowsOut: 1234 })).toBe(
		'<br/><span class="adhoc-perf-chip adhoc-perf-chip--time"><i class="bi bi-stopwatch"></i> 12ms</span>' +
			'<span class="adhoc-perf-chip adhoc-perf-chip--rows"><i class="bi bi-stack"></i> 1.2k</span>',
	);
});

test("formatNodeStatsSnippet: rowsOut-only emits the rows chip with the stack icon", () => {
	expect(formatNodeStatsSnippet({ rowsOut: 5 })).toBe('<br/><span class="adhoc-perf-chip adhoc-perf-chip--rows"><i class="bi bi-stack"></i> 5</span>');
});

test("formatNodeStatsSnippet: rowsIn produces a dedicated --rows-in chip (different colour / icon from rowsOut)", () => {
	expect(formatNodeStatsSnippet({ rowsIn: 10, rowsOut: 5 })).toBe(
		'<br/><span class="adhoc-perf-chip adhoc-perf-chip--rows"><i class="bi bi-stack"></i> 5</span>' +
			'<span class="adhoc-perf-chip adhoc-perf-chip--rows-in"><i class="bi bi-box-arrow-in-right"></i> 10</span>',
	);
});

test("planToMermaid: node label gains separate chips for each populated metric", () => {
	const plan = treeToGraph({ subject: "root", label: "root", state: "DONE", stats: { elapsedMs: 12, rowsOut: 1234 } });
	const out = planToMermaid(plan);
	expect(out).toContain(
		`n0["root<br/>` +
			`<span class="adhoc-perf-chip adhoc-perf-chip--time"><i class="bi bi-stopwatch"></i> 12ms</span>` +
			`<span class="adhoc-perf-chip adhoc-perf-chip--rows"><i class="bi bi-stack"></i> 1.2k</span>` +
			`"]`,
	);
});

test("STATE_PALETTE: every state has a non-empty fill / stroke / label (single source of truth for diagram + legend)", () => {
	for (const key of /** @type {const} */ (["done", "running", "pending", "failed"])) {
		expect(STATE_PALETTE[key].fill).toMatch(/^#/);
		expect(STATE_PALETTE[key].stroke).toMatch(/^#/);
		expect(STATE_PALETTE[key].label.length).toBeGreaterThan(0);
	}
});

test("planToMermaid: classDef lines are derived from STATE_PALETTE (no hardcoded duplicates)", () => {
	const out = planToMermaid(treeToGraph({ subject: "r", label: "r", state: "DONE" }));
	for (const key of /** @type {const} */ (["done", "running", "pending", "failed"])) {
		const p = STATE_PALETTE[key];
		expect(out).toContain(`classDef ${key} fill:${p.fill},stroke:${p.stroke},color:#000`);
	}
});

test("formatDetailsSnippet: empty / null details produce no snippet (no dangling <br/>)", () => {
	expect(formatDetailsSnippet(null)).toBe("");
	expect(formatDetailsSnippet(undefined)).toBe("");
	expect(formatDetailsSnippet({})).toBe("");
});

test("formatDetailsSnippet: one key=value line per non-sql entry, in iteration order", () => {
	expect(formatDetailsSnippet({ measure: "events", filter: "region=EU", groupBy: "year" })).toBe("<br/>measure=events<br/>filter=region=EU<br/>groupBy=year");
});

test("formatDetailsSnippet: sql key is filtered out (rendered separately via cylinder + side panel)", () => {
	expect(formatDetailsSnippet({ measure: "events", sql: "SELECT * FROM t" })).toBe("<br/>measure=events");
});

test("formatDetailsSnippet: null / empty values are skipped (no naked 'key=' lines)", () => {
	expect(formatDetailsSnippet({ measure: "events", filter: null, groupBy: "" })).toBe("<br/>measure=events");
});

test("formatDetailsSnippet: special characters in values are HTML-escaped via escapeLabel (brackets, quotes, pipes)", () => {
	expect(formatDetailsSnippet({ filter: "a[b]|c" })).toBe("<br/>filter=a(b)/c");
});

test("planToMermaid: CUBE_QUERY operator yields a distinct subroutine shape `[[…]]`", () => {
	const plan = treeToGraph({
		subject: "query-root",
		label: "CubeQuery on cube",
		operator: "CUBE_QUERY",
		state: "DONE",
		details: { measures: "[events]" },
	});
	const out = planToMermaid(plan);
	expect(out).toContain('n0[["CubeQuery on cube<br/>measures=(events)"]]');
	expect(out).toContain("classDef roleCubeQuery");
	expect(out).toContain("class n0 roleCubeQuery");
});

test("planToMermaid: per-step details map renders as multi-line label below the headline", () => {
	const plan = treeToGraph({ subject: "step", label: "events", operator: "CUBE_STEP", state: "DONE", details: { measure: "events", groupBy: "year" } });
	const out = planToMermaid(plan);
	expect(out).toContain('n0["events<br/>measure=events<br/>groupBy=year"]');
});

test("planToMermaid: a plan with cube / table-step / table-query nodes produces three engine-stage subgraphs", () => {
	const plan = treeToGraph({
		subject: "q",
		label: "CubeQuery on c",
		operator: "CUBE_QUERY",
		state: "DONE",
		children: [
			{
				subject: "cs",
				label: "events",
				operator: "CUBE_STEP",
				state: "DONE",
				children: [
					{
						subject: "ts-induced",
						label: "induced",
						operator: "TABLE_STEP",
						state: "DONE",
						children: [
							{
								subject: "ts-inducer",
								label: "inducer",
								operator: "TABLE_STEP",
								state: "DONE",
								children: [
									{
										subject: "v4",
										label: "V4",
										operator: "TABLE_QUERY",
										state: "DONE",
										children: [{ subject: "sql", label: "select", operator: "TABLE_QUERY", state: "DONE", details: { sql: "select 1" } }],
									},
								],
							},
						],
					},
				],
			},
		],
	});
	const out = planToMermaid(plan);
	expect(out).toContain('subgraph subCubeSteps ["Cube query steps"]');
	expect(out).toContain('subgraph subTableSteps ["Table query steps"]');
	expect(out).toContain('subgraph subTableQueries ["Table queries"]');
	expect(out.match(/^ {4}end$/gm)).toHaveLength(3);
	// CUBE_QUERY root is NOT inside a subgraph — heading sits at the top level.
	const cubeQueryDeclLine = out.split("\n").findIndex((l) => l.includes('CubeQuery on c"]]'));
	const firstSubgraphLine = out.split("\n").findIndex((l) => l.startsWith("    subgraph "));
	expect(cubeQueryDeclLine).toBeLessThan(firstSubgraphLine);
	expect(out).toMatch(/n\d+\s+-->\s+n\d+/);
});

test("planToMermaid: subgraphs are omitted when their bucket is empty (no dangling 'subgraph … end' boxes)", () => {
	const plan = treeToGraph({ subject: "r", label: "r", operator: "CUBE_STEP", state: "DONE" });
	const out = planToMermaid(plan);
	expect(out).toContain('subgraph subCubeSteps ["Cube query steps"]');
	expect(out).not.toContain("subTableSteps");
	expect(out).not.toContain("subTableQueries");
	expect(out.match(/^ {4}end$/gm)).toHaveLength(1);
});

test("planToMermaid: TABLE_QUERY V4 and SQL-leaf children share the same `Table queries` subgraph", () => {
	const plan = treeToGraph({
		subject: "v4",
		label: "V4",
		operator: "TABLE_QUERY",
		state: "DONE",
		children: [{ subject: "sql", label: "select", operator: "TABLE_QUERY", state: "DONE", details: { sql: "select 1" } }],
	});
	const out = planToMermaid(plan);
	expect(out).toContain('subgraph subTableQueries ["Table queries"]');
	const lines = out.split("\n");
	const start = lines.findIndex((l) => l.includes('subgraph subTableQueries ["Table queries"]'));
	const end = lines.findIndex((l, i) => i > start && l.trim() === "end");
	const slice = lines.slice(start, end + 1).join("\n");
	expect(slice).toMatch(/\{\{"V4"\}\}/);
	expect(slice).toMatch(/\[\("select"\)\]/);
});

test("planToMermaid: a shared SQL leaf with N incoming edges produces ONE cylinder + N edges", () => {
	// Graph fixture built directly: server-side projector has already deduped. The SPA renders one node declaration
	// and one edge per `edges` entry.
	const plan = {
		rootId: "n0",
		nodes: [
			{ id: "n0", subject: "q", label: "CubeQuery", operator: "CUBE_QUERY", state: "DONE" },
			{ id: "n1", subject: "p1", label: "p1", operator: "CUBE_STEP", state: "DONE" },
			{ id: "n2", subject: "p2", label: "p2", operator: "CUBE_STEP", state: "DONE" },
			{ id: "n3", subject: "p3", label: "p3", operator: "CUBE_STEP", state: "DONE" },
			{ id: "n4", subject: "sql", label: "select", operator: "TABLE_QUERY", state: "DONE", details: { sql: "SELECT k1 FROM t" } },
		],
		edges: [
			{ parentId: "n0", childId: "n1" },
			{ parentId: "n0", childId: "n2" },
			{ parentId: "n0", childId: "n3" },
			{ parentId: "n1", childId: "n4" },
			{ parentId: "n2", childId: "n4" },
			{ parentId: "n3", childId: "n4" },
		],
	};
	const out = planToMermaid(plan);
	// Exactly one SQL-leaf node declaration (cylinder shape `[(…)]`).
	expect(out.match(/\[\("select"\)\]/g)).toHaveLength(1);
	// Three edges into n4 — one from each of n1 / n2 / n3.
	const edgesIntoSql = out.split("\n").filter((l) => /-->\s+n4$/.test(l));
	expect(edgesIntoSql).toHaveLength(3);
});

test("planToMermaid: PENDING / no-stats node renders the label unchanged (no dangling <br/>)", () => {
	const plan = treeToGraph({ subject: "root", label: "the-root", state: "PENDING" });
	const out = planToMermaid(plan);
	expect(out).toContain('n0["the-root"]');
	expect(out).not.toContain("<br/>");
});

test("single-node plan emits one node + one classDef + one class line", () => {
	const out = planToMermaid(treeToGraph({ subject: "root", label: "the-root", state: "DONE" }));
	expect(out).toContain("graph TD");
	expect(out).toContain('n0["the-root"]');
	expect(out).toContain("class n0 done");
	expect(out).toContain("classDef done fill:");
});

test("parent → child produces an edge declaration", () => {
	const plan = treeToGraph({
		subject: "root",
		label: "root",
		state: "RUNNING",
		children: [{ subject: "leaf", label: "leaf", state: "PENDING" }],
	});
	const out = planToMermaid(plan);
	expect(out).toMatch(/n0\s+-->\s+n1/);
	expect(out).toContain("class n0 running");
	expect(out).toContain("class n1 pending");
});

test("DAG fan-in: a shared node with two incoming edges renders one declaration + two edges", () => {
	// Graph form (projector has already deduped). Both `a` and `b` point at the same leaf id `n3`.
	const plan = {
		rootId: "n0",
		nodes: [
			{ id: "n0", subject: "root", label: "root", state: "DONE" },
			{ id: "n1", subject: "a", label: "a", state: "DONE" },
			{ id: "n2", subject: "b", label: "b", state: "DONE" },
			{ id: "n3", subject: "leaf", label: "leaf", state: "DONE" },
		],
		edges: [
			{ parentId: "n0", childId: "n1" },
			{ parentId: "n0", childId: "n2" },
			{ parentId: "n1", childId: "n3" },
			{ parentId: "n2", childId: "n3" },
		],
	};
	const out = planToMermaid(plan);
	// "leaf" declared exactly once.
	expect(out.match(/\["leaf"\]/g)).toHaveLength(1);
	// 4 edges total.
	const edges = out.split("\n").filter((l) => l.includes(" --> "));
	expect(edges).toHaveLength(4);
});

test("FAILED state is rendered with the failed CSS class", () => {
	const plan = treeToGraph({ subject: "x", label: "x", state: "FAILED" });
	expect(planToMermaid(plan)).toContain("class n0 failed");
});

test("label containing brackets and quotes is escaped so Mermaid can parse it", () => {
	const plan = treeToGraph({ subject: "x", label: 'a [bracket] and "quote"', state: "DONE" });
	const out = planToMermaid(plan);
	expect(out).toContain("a (bracket) and &quot;quote&quot;");
	expect(out).not.toContain('"quote"');
});

// ---------------------------------------------------------------------------------------------
// Semantic shape & role-class — every operator picks a distinct Mermaid bracket shape, and the resulting classDef
// bucket reflects the role. State-class assignment is independent (so a node can be both `done` AND `roleSql`).
// ---------------------------------------------------------------------------------------------

test("CUBE_STEP operator → rectangle [...] + roleCube class", () => {
	const out = planToMermaid(treeToGraph({ subject: "x", label: "agg", operator: "CUBE_STEP", state: "DONE" }));
	expect(out).toContain('n0["agg"]');
	expect(out).toContain("class n0 roleCube");
});

test("TABLE_STEP operator → stadium ([...]) + roleTable class", () => {
	const out = planToMermaid(treeToGraph({ subject: "x", label: "t-step", operator: "TABLE_STEP", state: "DONE" }));
	expect(out).toContain('n0(["t-step"])');
	expect(out).toContain("class n0 roleTable");
});

test("TABLE_QUERY operator (no SQL details) → hexagon {{...}} + roleTableQuery", () => {
	const out = planToMermaid(treeToGraph({ subject: "x", label: "v4", operator: "TABLE_QUERY", state: "DONE" }));
	expect(out).toContain('n0{{"v4"}}');
	expect(out).toContain("class n0 roleTableQuery");
});

test("node carrying details.sql → cylinder [(...)] + roleSql class, regardless of operator", () => {
	const plan = treeToGraph({
		subject: "x",
		label: "select sum(k) from t",
		operator: "TABLE_QUERY",
		state: "DONE",
		details: { language: "sql", sql: "select sum(k) from t" },
	});
	const out = planToMermaid(plan);
	expect(out).toContain('n0[("select sum(k) from t<br/>language=sql")]');
	expect(out).toContain("class n0 roleSql");
});

test("unknown / missing operator falls back to rectangle + roleOther", () => {
	const out = planToMermaid(treeToGraph({ subject: "x", label: "x", state: "DONE" }));
	expect(out).toContain('n0["x"]');
	expect(out).toContain("class n0 roleOther");
});

test("state and role classes accumulate on the same node", () => {
	const out = planToMermaid(treeToGraph({ subject: "x", label: "x", operator: "CUBE_STEP", state: "RUNNING" }));
	expect(out).toContain("class n0 running");
	expect(out).toContain("class n0 roleCube");
});

// ---------------------------------------------------------------------------------------------
// collectSqlLeaves — flat filter over the nodes list. Tests pin document order + dedup-by-SQL.
// ---------------------------------------------------------------------------------------------

test("collectSqlLeaves returns empty array on missing plan", () => {
	expect(collectSqlLeaves(/** @type {any} */ (null))).toEqual([]);
	expect(collectSqlLeaves(/** @type {any} */ ({ nodes: null }))).toEqual([]);
});

test("collectSqlLeaves returns one entry per SQL leaf, in nodes-list order", () => {
	const plan = treeToGraph({
		subject: "root",
		label: "root",
		state: "DONE",
		children: [
			{
				subject: "v4-a",
				label: "v4-a",
				state: "DONE",
				children: [{ subject: "sql-a", label: "select sum(k) from a", state: "DONE", details: { language: "sql", sql: "select sum(k) from a" } }],
			},
			{
				subject: "v4-b",
				label: "v4-b",
				state: "DONE",
				children: [{ subject: "sql-b", label: "select count(*) from b", state: "DONE", details: { language: "sql", sql: "select count(*) from b" } }],
			},
		],
	});
	const leaves = collectSqlLeaves(plan);
	expect(leaves).toHaveLength(2);
	expect(leaves[0]).toEqual({ label: "select sum(k) from a", sql: "select sum(k) from a" });
	expect(leaves[1]).toEqual({ label: "select count(*) from b", sql: "select count(*) from b" });
});

test("collectSqlLeaves dedupes by SQL text — one entry per distinct SQL string", () => {
	// Two parents share the same SQL — projector emits one node with two incoming edges; SPA dedup gives one
	// entry in the leaves list.
	const plan = {
		rootId: "n0",
		nodes: [
			{ id: "n0", subject: "root", label: "root", state: "DONE" },
			{ id: "n1", subject: "a", label: "a", state: "DONE" },
			{ id: "n2", subject: "b", label: "b", state: "DONE" },
			{ id: "n3", subject: "sql", label: "select sum(k) from t", state: "DONE", details: { language: "sql", sql: "select sum(k) from t" } },
		],
		edges: [
			{ parentId: "n0", childId: "n1" },
			{ parentId: "n0", childId: "n2" },
			{ parentId: "n1", childId: "n3" },
			{ parentId: "n2", childId: "n3" },
		],
	};
	const leaves = collectSqlLeaves(plan);
	expect(leaves).toHaveLength(1);
	expect(leaves[0].sql).toBe("select sum(k) from t");
});

test("collectSqlLeaves ignores nodes whose details have no sql key", () => {
	const plan = treeToGraph({ subject: "root", label: "root", state: "DONE", details: { something: "else" } });
	expect(collectSqlLeaves(plan)).toEqual([]);
});

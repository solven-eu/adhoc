// @ts-check
import { expect, test } from "vitest";

import { planToMermaid, collectSqlLeaves } from "@/js/adhoc-query-plan-mermaid.js";

// Tests for the pure plan → Mermaid converter. No DOM, no mermaid runtime — just string output we
// can inspect.

test("empty / missing plan emits a header-only graph (no nodes)", () => {
	// JSDoc types don't reject nullish here (the converter validates at runtime).
	expect(planToMermaid(/** @type {any} */ (null))).toBe("graph TD\n");
});

test("single-node plan emits one node + one classDef + one class line", () => {
	const plan = { root: { subject: "root", label: "the-root", state: "DONE", children: [] } };
	const out = planToMermaid(plan);
	expect(out).toContain("graph TD");
	expect(out).toContain('n0["the-root"]');
	expect(out).toContain("class n0 done");
	expect(out).toContain("classDef done fill:");
});

test("parent → child produces an edge declaration", () => {
	const plan = {
		root: {
			subject: "root",
			label: "root",
			state: "RUNNING",
			children: [{ subject: "leaf", label: "leaf", state: "PENDING", children: [] }],
		},
	};
	const out = planToMermaid(plan);
	expect(out).toMatch(/n0\s+-->\s+n1/);
	expect(out).toContain("class n0 running");
	expect(out).toContain("class n1 pending");
});

test("shared child is rendered once with two incoming edges (DAG, not tree)", () => {
	const leaf = { subject: "leaf", label: "leaf", state: "DONE", children: [] };
	const plan = {
		root: {
			subject: "root",
			label: "root",
			state: "DONE",
			children: [
				{ subject: "a", label: "a", state: "DONE", children: [leaf] },
				{ subject: "b", label: "b", state: "DONE", children: [leaf] },
			],
		},
	};
	const out = planToMermaid(plan);
	// "leaf" declared exactly once
	expect(out.match(/\["leaf"\]/g)).toHaveLength(1);
	// 4 edges: root→a, root→b, a→leaf, b→leaf (the leaf is shared so both `a→leaf` and `b→leaf`
	// are emitted — Mermaid renders these as two arrows landing on the same node).
	const edges = out.split("\n").filter((l) => l.includes(" --> "));
	expect(edges).toHaveLength(4);
});

test("FAILED state is rendered with the failed CSS class", () => {
	const plan = { root: { subject: "x", label: "x", state: "FAILED", children: [] } };
	expect(planToMermaid(plan)).toContain("class n0 failed");
});

test("label containing brackets and quotes is escaped so Mermaid can parse it", () => {
	const plan = {
		root: { subject: "x", label: 'a [bracket] and "quote"', state: "DONE", children: [] },
	};
	const out = planToMermaid(plan);
	expect(out).toContain("a (bracket) and &quot;quote&quot;");
	// Raw double quote inside the label would break Mermaid — make sure none escaped.
	expect(out).not.toContain('"quote"');
});

test("nodes with no explicit subject fall back to label as the dedup key", () => {
	const plan = {
		root: {
			label: "root",
			state: "DONE",
			children: [{ label: "leaf", state: "DONE", children: [] }],
		},
	};
	const out = planToMermaid(plan);
	expect(out).toContain('n0["root"]');
	expect(out).toContain('n1["leaf"]');
});

// ---------------------------------------------------------------------------------------------
// Semantic shape & role-class — every operator picks a distinct Mermaid bracket shape, and the
// resulting classDef bucket reflects the role. State-class assignment is independent (so a node
// can be both `done` AND `roleSql`).
// ---------------------------------------------------------------------------------------------

test("CUBE_STEP operator → rectangle [...] + roleCube class", () => {
	const plan = {
		root: { subject: "x", label: "agg", operator: "CUBE_STEP", state: "DONE", children: [] },
	};
	const out = planToMermaid(plan);
	expect(out).toContain('n0["agg"]');
	expect(out).toContain("class n0 roleCube");
});

test("TABLE_STEP operator → stadium ([...]) + roleTable class", () => {
	const plan = {
		root: { subject: "x", label: "t-step", operator: "TABLE_STEP", state: "DONE", children: [] },
	};
	const out = planToMermaid(plan);
	expect(out).toContain('n0(["t-step"])');
	expect(out).toContain("class n0 roleTable");
});

test("TABLE_QUERY operator (no SQL details) → hexagon {{...}} + roleTableQuery", () => {
	const plan = {
		root: { subject: "x", label: "v4", operator: "TABLE_QUERY", state: "DONE", children: [] },
	};
	const out = planToMermaid(plan);
	expect(out).toContain('n0{{"v4"}}');
	expect(out).toContain("class n0 roleTableQuery");
});

test("node carrying details.sql → cylinder [(...)] + roleSql class, regardless of operator", () => {
	// `details.sql` wins over the operator tag: the wrapper publishes a TABLE_QUERY operator on the
	// SQL leaf today, but the SQL-vs-merged distinction matters more for rendering.
	const plan = {
		root: {
			subject: "x",
			label: "select sum(k) from t",
			operator: "TABLE_QUERY",
			state: "DONE",
			details: { language: "sql", sql: "select sum(k) from t" },
			children: [],
		},
	};
	const out = planToMermaid(plan);
	expect(out).toContain('n0[("select sum(k) from t")]');
	expect(out).toContain("class n0 roleSql");
});

test("unknown / missing operator falls back to rectangle + roleOther", () => {
	const plan = { root: { subject: "x", label: "x", state: "DONE", children: [] } };
	const out = planToMermaid(plan);
	expect(out).toContain('n0["x"]');
	expect(out).toContain("class n0 roleOther");
});

test("state and role classes accumulate on the same node", () => {
	const plan = {
		root: { subject: "x", label: "x", operator: "CUBE_STEP", state: "RUNNING", children: [] },
	};
	const out = planToMermaid(plan);
	expect(out).toContain("class n0 running");
	expect(out).toContain("class n0 roleCube");
});

// ---------------------------------------------------------------------------------------------
// collectSqlLeaves — walk-and-dedup helper used by the modal to render the SQL list with copy
// buttons. Tests pin the document-order traversal and the dedup-on-SQL behaviour.
// ---------------------------------------------------------------------------------------------

test("collectSqlLeaves returns empty array on missing plan", () => {
	expect(collectSqlLeaves(/** @type {any} */ (null))).toEqual([]);
	expect(collectSqlLeaves(/** @type {any} */ ({ root: null }))).toEqual([]);
});

test("collectSqlLeaves returns one entry per SQL leaf, in document order", () => {
	const plan = {
		root: {
			subject: "root",
			label: "root",
			state: "DONE",
			children: [
				{
					subject: "v4-a",
					label: "v4-a",
					state: "DONE",
					children: [
						{
							subject: "sql-a",
							label: "select sum(k) from a",
							state: "DONE",
							details: { language: "sql", sql: "select sum(k) from a" },
							children: [],
						},
					],
				},
				{
					subject: "v4-b",
					label: "v4-b",
					state: "DONE",
					children: [
						{
							subject: "sql-b",
							label: "select count(*) from b",
							state: "DONE",
							details: { language: "sql", sql: "select count(*) from b" },
							children: [],
						},
					],
				},
			],
		},
	};
	const leaves = collectSqlLeaves(plan);
	expect(leaves).toHaveLength(2);
	expect(leaves[0]).toEqual({ label: "select sum(k) from a", sql: "select sum(k) from a" });
	expect(leaves[1]).toEqual({ label: "select count(*) from b", sql: "select count(*) from b" });
});

test("collectSqlLeaves dedupes by SQL text — same query reached twice via shared child appears once", () => {
	const shared = {
		subject: "sql",
		label: "select sum(k) from t",
		state: "DONE",
		details: { language: "sql", sql: "select sum(k) from t" },
		children: [],
	};
	const plan = {
		root: {
			subject: "root",
			label: "root",
			state: "DONE",
			children: [
				{ subject: "a", label: "a", state: "DONE", children: [shared] },
				{ subject: "b", label: "b", state: "DONE", children: [shared] },
			],
		},
	};
	const leaves = collectSqlLeaves(plan);
	expect(leaves).toHaveLength(1);
	expect(leaves[0].sql).toBe("select sum(k) from t");
});

test("collectSqlLeaves ignores nodes whose details have no sql key", () => {
	const plan = {
		root: {
			subject: "root",
			label: "root",
			state: "DONE",
			details: { something: "else" },
			children: [],
		},
	};
	expect(collectSqlLeaves(plan)).toEqual([]);
});

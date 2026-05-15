// @ts-check
import { expect, test } from "vitest";

import { planToMermaid } from "@/js/adhoc-query-plan-mermaid.js";

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

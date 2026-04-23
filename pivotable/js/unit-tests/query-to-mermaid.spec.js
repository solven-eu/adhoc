import { expect, test } from "vitest";

import { queryModelToMermaid } from "@/js/adhoc-query-to-mermaid.js";

// Helper — minimal queryModel shape, matching `queryHelper.makeQueryModel()`.
function model() {
	return {
		selectedMeasures: {},
		selectedColumnsOrdered: [],
		withStarColumns: {},
		filter: {},
	};
}

test("empty queryModel renders a lone cube node", () => {
	const md = queryModelToMermaid(model(), "simple");
	expect(md).toEqual(["flowchart LR", '  cube(["simple"])'].join("\n"));
});

test("cube name falls back to the literal 'cube' when none is passed", () => {
	const md = queryModelToMermaid(model(), "");
	expect(md).toEqual(["flowchart LR", '  cube(["cube"])'].join("\n"));
});

test("single selected measure hangs off the cube (no groupBy)", () => {
	const q = model();
	q.selectedMeasures.delta = true;
	const md = queryModelToMermaid(q, "simple");
	expect(md).toEqual(
		["flowchart LR", '  cube(["simple"])', '  subgraph measures["Measures"]', "    direction TB", '    m_delta["delta"]', "  end", "  cube --> measures"].join(
			"\n",
		),
	);
});

test("unselected measures are skipped", () => {
	const q = model();
	q.selectedMeasures.delta = true;
	q.selectedMeasures.vega = false;
	const md = queryModelToMermaid(q, "simple");
	expect(md).toContain('m_delta["delta"]');
	expect(md).not.toContain("m_vega");
});

test("selected columns render as a GroupBy subgraph, preserving order", () => {
	const q = model();
	q.selectedColumnsOrdered.push("city", "country");
	const md = queryModelToMermaid(q, "simple");
	expect(md).toEqual(
		[
			"flowchart LR",
			'  cube(["simple"])',
			'  subgraph groupBy["Group By"]',
			"    direction TB",
			'    gb_city["city"]',
			'    gb_country["country"]',
			"  end",
			"  cube --> groupBy",
		].join("\n"),
	);
});

test("withStar column carries a trailing '*' in the groupBy label", () => {
	const q = model();
	q.selectedColumnsOrdered.push("city");
	q.withStarColumns.city = true;
	const md = queryModelToMermaid(q, "simple");
	expect(md).toContain('gb_city["city *"]');
});

test("measures come after groupBy in the sandwich chain", () => {
	const q = model();
	q.selectedColumnsOrdered.push("city");
	q.selectedMeasures.delta = true;
	const md = queryModelToMermaid(q, "simple");
	// The edge from groupBy → measures must be present (rather than cube → measures).
	expect(md).toContain("cube --> groupBy");
	expect(md).toContain("groupBy --> measures");
	expect(md).not.toContain("cube --> measures");
});

test("single column=value filter renders as a leaf edge into the cube", () => {
	const q = model();
	q.selectedMeasures.delta = true;
	q.filter = { type: "column", column: "country", valueMatcher: "France" };
	const md = queryModelToMermaid(q, "simple");
	expect(md).toContain('f1["country = France"]');
	expect(md).toContain("f1 --> cube");
});

test("AND of two column filters wraps them in an AND subgraph", () => {
	const q = model();
	q.filter = {
		type: "and",
		filters: [
			{ type: "column", column: "country", valueMatcher: "France" },
			{ type: "column", column: "city", valueMatcher: "Paris" },
		],
	};
	const md = queryModelToMermaid(q, "simple");
	expect(md).toContain('subgraph gAnd1["AND"]');
	expect(md).toContain('f2["country = France"]');
	expect(md).toContain('f3["city = Paris"]');
	expect(md).toContain("gAnd1 --> cube");
});

test("NOT wraps its child in a NOT subgraph", () => {
	const q = model();
	q.filter = { type: "not", negated: { type: "column", column: "country", valueMatcher: "France" } };
	const md = queryModelToMermaid(q, "simple");
	expect(md).toContain('subgraph gNot1["NOT"]');
	expect(md).toContain('f2["country = France"]');
	expect(md).toContain("gNot1 --> cube");
});

test("disabled filter nodes are stripped before rendering", () => {
	const q = model();
	q.filter = {
		type: "and",
		filters: [
			{ type: "column", column: "country", valueMatcher: "France" },
			{ type: "column", column: "city", valueMatcher: "Paris", disabled: true },
		],
	};
	const md = queryModelToMermaid(q, "simple");
	// Only the enabled leaf survives; the wrapping AND collapses to the single remaining leaf.
	expect(md).toContain('f1["country = France"]');
	expect(md).not.toContain("city = Paris");
	expect(md).not.toContain("AND");
});

test("disabled root filter is skipped entirely (no Filter stage)", () => {
	const q = model();
	q.filter = { type: "column", column: "country", valueMatcher: "France", disabled: true };
	const md = queryModelToMermaid(q, "simple");
	expect(md).not.toContain("country = France");
	// No edge into the cube either.
	expect(md).not.toContain("--> cube");
});

test("null valueMatcher renders as `col = ?` (same as the SQL converter's fallback)", () => {
	const q = model();
	q.filter = { type: "column", column: "country", valueMatcher: null };
	const md = queryModelToMermaid(q, "simple");
	// The shared `extractEqualsValue` helper maps both null and undefined to `undefined`,
	// so a bare `null` falls into the "unknown matcher" branch rather than `IS NULL`.
	// This matches `adhoc-query-to-sql.js` behaviour.
	expect(md).toContain("country = ?");
});

test("unknown valueMatcher shape renders as `col = ?` so the user sees it", () => {
	const q = model();
	q.filter = { type: "column", column: "country", valueMatcher: { type: "like", pattern: "FRA%" } };
	const md = queryModelToMermaid(q, "simple");
	expect(md).toContain("country = ?");
});

test("identifier with spaces slugifies into a valid mermaid id", () => {
	const q = model();
	q.selectedColumnsOrdered.push("postal code");
	const md = queryModelToMermaid(q, "simple");
	// Label retains the original text; node id replaces disallowed characters.
	expect(md).toContain('gb_postal_code["postal code"]');
});

test("double-quote inside a label escapes to &quot;", () => {
	const q = model();
	q.filter = { type: "column", column: "name", valueMatcher: 'O"Brien' };
	const md = queryModelToMermaid(q, "simple");
	expect(md).toContain('f1["name = O&quot;Brien"]');
});

test("full sandwich: filter + groupBy + measures renders all three stages in order", () => {
	const q = model();
	q.filter = { type: "column", column: "country", valueMatcher: "France" };
	q.selectedColumnsOrdered.push("city");
	q.selectedMeasures.delta = true;
	const md = queryModelToMermaid(q, "simple");
	expect(md).toEqual(
		[
			"flowchart LR",
			'  f1["country = France"]',
			'  cube(["simple"])',
			'  subgraph groupBy["Group By"]',
			"    direction TB",
			'    gb_city["city"]',
			"  end",
			'  subgraph measures["Measures"]',
			"    direction TB",
			'    m_delta["delta"]',
			"  end",
			"  f1 --> cube",
			"  cube --> groupBy",
			"  groupBy --> measures",
		].join("\n"),
	);
});

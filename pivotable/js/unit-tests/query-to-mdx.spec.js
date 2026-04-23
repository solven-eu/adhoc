import { expect, test } from "vitest";

import { queryModelToMdx } from "@/js/adhoc-query-to-mdx.js";

// Helper — build a minimal queryModel structure matching what `queryHelper.makeQueryModel()`
// would produce. Tests write to the returned object directly; no reactivity needed here.
function model() {
	return {
		selectedMeasures: {},
		selectedColumnsOrdered: [],
		withStarColumns: {},
		filter: {},
	};
}

test("empty queryModel yields bare SELECT FROM", () => {
	const mdx = queryModelToMdx(model(), "simple");
	expect(mdx).toEqual(["SELECT", "FROM [simple]"].join("\n"));
});

test("single measure lands on COLUMNS", () => {
	const q = model();
	q.selectedMeasures.delta = true;
	const mdx = queryModelToMdx(q, "simple");
	expect(mdx).toEqual(["SELECT", "  {[Measures].[delta]} ON COLUMNS", "FROM [simple]"].join("\n"));
});

test("multiple selected measures are comma-separated, unselected skipped", () => {
	const q = model();
	q.selectedMeasures.delta = true;
	q.selectedMeasures.gamma = true;
	q.selectedMeasures.vega = false; // should be skipped
	const mdx = queryModelToMdx(q, "simple");
	expect(mdx).toEqual(["SELECT", "  {[Measures].[delta], [Measures].[gamma]} ON COLUMNS", "FROM [simple]"].join("\n"));
});

test("single groupBy column goes on ROWS via .Members with NON EMPTY", () => {
	const q = model();
	q.selectedColumnsOrdered.push("city");
	const mdx = queryModelToMdx(q, "simple");
	expect(mdx).toEqual(["SELECT", "  NON EMPTY {[city].[city].Members} ON ROWS", "FROM [simple]"].join("\n"));
});

test("two groupBy columns produce a multi-line NON EMPTY CrossJoin", () => {
	const q = model();
	q.selectedColumnsOrdered.push("city", "country");
	const mdx = queryModelToMdx(q, "simple");
	expect(mdx).toEqual(
		["SELECT", "  NON EMPTY CrossJoin(", "    {[city].[city].Members},", "    {[country].[country].Members}", "  ) ON ROWS", "FROM [simple]"].join("\n"),
	);
});

test("measures + columns produce both axes, comma-separated", () => {
	const q = model();
	q.selectedMeasures.delta = true;
	q.selectedColumnsOrdered.push("city");
	const mdx = queryModelToMdx(q, "simple");
	expect(mdx).toEqual(["SELECT", "  {[Measures].[delta]} ON COLUMNS,", "  NON EMPTY {[city].[city].Members} ON ROWS", "FROM [simple]"].join("\n"));
});

test("single column=value filter becomes a WHERE tuple with Atoti AllMember path", () => {
	const q = model();
	q.selectedMeasures.delta = true;
	q.filter = { type: "column", column: "country", valueMatcher: "France" };
	const mdx = queryModelToMdx(q, "simple");
	expect(mdx).toEqual(["SELECT", "  {[Measures].[delta]} ON COLUMNS", "FROM [simple]", "WHERE ([country].[AllMember].[France])"].join("\n"));
});

test("AND of column=value filters compose into a multi-line WHERE tuple", () => {
	const q = model();
	q.selectedMeasures.delta = true;
	q.filter = {
		type: "and",
		filters: [
			{ type: "column", column: "country", valueMatcher: "France" },
			{ type: "column", column: "ccy", valueMatcher: "EUR" },
		],
	};
	const mdx = queryModelToMdx(q, "simple");
	expect(mdx).toEqual(
		["SELECT", "  {[Measures].[delta]} ON COLUMNS", "FROM [simple]", "WHERE (", "  [country].[AllMember].[France],", "  [ccy].[AllMember].[EUR]", ")"].join(
			"\n",
		),
	);
});

test("equals-matcher object form is accepted (type: 'equals')", () => {
	const q = model();
	q.filter = { type: "column", column: "country", valueMatcher: { type: "equals", operand: "France" } };
	const mdx = queryModelToMdx(q, "simple");
	expect(mdx).toContain("WHERE ([country].[AllMember].[France])");
});

test("OR filters fall back to an MDX comment (not representable in minimal MDX)", () => {
	const q = model();
	q.filter = {
		type: "or",
		filters: [
			{ type: "column", column: "ccy", valueMatcher: "EUR" },
			{ type: "column", column: "ccy", valueMatcher: "USD" },
		],
	};
	const mdx = queryModelToMdx(q, "simple");
	expect(mdx).toContain("/* filter not representable in minimal MDX:");
	expect(mdx).not.toContain("WHERE");
});

test("NOT filter falls back to a comment", () => {
	const q = model();
	q.filter = { type: "not", negated: { type: "column", column: "ccy", valueMatcher: "EUR" } };
	const mdx = queryModelToMdx(q, "simple");
	expect(mdx).toContain("/* filter not representable in minimal MDX:");
});

test("disabled filters are stripped before translation", () => {
	const q = model();
	q.filter = {
		type: "and",
		filters: [
			{ type: "column", column: "country", valueMatcher: "France" },
			// `disabled: true` — must not reach the MDX output
			{ type: "column", column: "ccy", valueMatcher: "EUR", disabled: true },
		],
	};
	const mdx = queryModelToMdx(q, "simple");
	expect(mdx).toContain("WHERE ([country].[AllMember].[France])");
	expect(mdx).not.toContain("EUR");
});

test("a fully-disabled filter yields no WHERE clause at all", () => {
	const q = model();
	q.filter = { type: "column", column: "country", valueMatcher: "France", disabled: true };
	const mdx = queryModelToMdx(q, "simple");
	expect(mdx).not.toContain("WHERE");
});

test("`]` in names is escaped by doubling", () => {
	const q = model();
	q.selectedMeasures["weird]name"] = true;
	q.selectedColumnsOrdered.push("col]umn");
	q.filter = { type: "column", column: "col]umn", valueMatcher: "val]ue" };
	const mdx = queryModelToMdx(q, "cub]e");
	expect(mdx).toContain("[Measures].[weird]]name]");
	expect(mdx).toContain("{[col]]umn].[col]]umn].Members}");
	expect(mdx).toContain("[col]]umn].[AllMember].[val]]ue]");
	expect(mdx).toContain("FROM [cub]]e]");
});

test("no measures, no columns, no filter: minimal SELECT FROM", () => {
	const mdx = queryModelToMdx(model(), "");
	expect(mdx).toEqual(["SELECT", "FROM []"].join("\n"));
});

test("withStarColumns renders as DrillDownMember from the All member", () => {
	const q = model();
	q.selectedColumnsOrdered.push("city");
	q.withStarColumns.city = true;
	const mdx = queryModelToMdx(q, "simple");
	expect(mdx).toEqual(["SELECT", "  NON EMPTY DrillDownMember({[city].[city].[AllMember]}, {[city].[city].[AllMember]}) ON ROWS", "FROM [simple]"].join("\n"));
});

test("withStarColumns mixes with plain members in a multi-line NON EMPTY CrossJoin", () => {
	const q = model();
	q.selectedColumnsOrdered.push("city", "country");
	q.withStarColumns.country = true;
	const mdx = queryModelToMdx(q, "simple");
	expect(mdx).toEqual(
		[
			"SELECT",
			"  NON EMPTY CrossJoin(",
			"    {[city].[city].Members},",
			"    DrillDownMember({[country].[country].[AllMember]}, {[country].[country].[AllMember]})",
			"  ) ON ROWS",
			"FROM [simple]",
		].join("\n"),
	);
});

test("withStarColumns=false (falsy) is equivalent to not set", () => {
	const q = model();
	q.selectedColumnsOrdered.push("city");
	q.withStarColumns.city = false;
	const mdx = queryModelToMdx(q, "simple");
	expect(mdx).toContain("{[city].[city].Members}");
	expect(mdx).not.toContain("DrillDownMember");
});

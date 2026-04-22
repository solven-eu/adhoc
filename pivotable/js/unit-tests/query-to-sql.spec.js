import { expect, test } from "vitest";

import { queryModelToSql } from "@/js/adhoc-query-to-sql.js";

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

test("empty queryModel yields SELECT * FROM", () => {
	const sql = queryModelToSql(model(), "simple");
	expect(sql).toEqual(["SELECT *", 'FROM "simple"'].join("\n"));
});

test("single measure becomes a SELECT identifier", () => {
	const q = model();
	q.selectedMeasures.delta = true;
	const sql = queryModelToSql(q, "simple");
	expect(sql).toEqual(["SELECT", '  "delta"', 'FROM "simple"'].join("\n"));
});

test("multiple selected measures are comma-separated, unselected skipped", () => {
	const q = model();
	q.selectedMeasures.delta = true;
	q.selectedMeasures.gamma = true;
	q.selectedMeasures.vega = false; // should be skipped
	const sql = queryModelToSql(q, "simple");
	expect(sql).toEqual(["SELECT", '  "delta",', '  "gamma"', 'FROM "simple"'].join("\n"));
});

test("single groupBy column produces SELECT + GROUP BY", () => {
	const q = model();
	q.selectedColumnsOrdered.push("city");
	const sql = queryModelToSql(q, "simple");
	expect(sql).toEqual(["SELECT", '  "city"', 'FROM "simple"', 'GROUP BY "city"'].join("\n"));
});

test("multiple groupBy columns order-preserved in SELECT and GROUP BY", () => {
	const q = model();
	q.selectedColumnsOrdered.push("city", "country");
	const sql = queryModelToSql(q, "simple");
	expect(sql).toEqual(["SELECT", '  "city",', '  "country"', 'FROM "simple"', 'GROUP BY "city", "country"'].join("\n"));
});

test("measures + columns: columns come first in SELECT", () => {
	const q = model();
	q.selectedMeasures.delta = true;
	q.selectedColumnsOrdered.push("city");
	const sql = queryModelToSql(q, "simple");
	expect(sql).toEqual(["SELECT", '  "city",', '  "delta"', 'FROM "simple"', 'GROUP BY "city"'].join("\n"));
});

test("single column=value filter becomes a WHERE equality with a quoted literal", () => {
	const q = model();
	q.selectedMeasures.delta = true;
	q.filter = { type: "column", column: "country", valueMatcher: "France" };
	const sql = queryModelToSql(q, "simple");
	expect(sql).toEqual(["SELECT", '  "delta"', 'FROM "simple"', `WHERE "country" = 'France'`].join("\n"));
});

test("AND of column=value filters compose into an ANDed WHERE clause", () => {
	const q = model();
	q.selectedMeasures.delta = true;
	q.filter = {
		type: "and",
		filters: [
			{ type: "column", column: "country", valueMatcher: "France" },
			{ type: "column", column: "ccy", valueMatcher: "EUR" },
		],
	};
	const sql = queryModelToSql(q, "simple");
	expect(sql).toContain(`WHERE ("country" = 'France') AND ("ccy" = 'EUR')`);
});

test("OR of column=value filters compose into an ORed WHERE clause", () => {
	const q = model();
	q.filter = {
		type: "or",
		filters: [
			{ type: "column", column: "ccy", valueMatcher: "EUR" },
			{ type: "column", column: "ccy", valueMatcher: "USD" },
		],
	};
	const sql = queryModelToSql(q, "simple");
	expect(sql).toContain(`WHERE ("ccy" = 'EUR') OR ("ccy" = 'USD')`);
});

test("NOT filter becomes NOT (...)", () => {
	const q = model();
	q.filter = { type: "not", negated: { type: "column", column: "ccy", valueMatcher: "EUR" } };
	const sql = queryModelToSql(q, "simple");
	expect(sql).toContain(`WHERE NOT ("ccy" = 'EUR')`);
});

test("equals-matcher object form is accepted (type: 'equals')", () => {
	const q = model();
	q.filter = { type: "column", column: "country", valueMatcher: { type: "equals", operand: "France" } };
	const sql = queryModelToSql(q, "simple");
	expect(sql).toContain(`WHERE "country" = 'France'`);
});

test("unknown matcher falls back to a SQL comment (not representable in minimal SQL)", () => {
	const q = model();
	q.filter = {
		type: "column",
		column: "ccy",
		valueMatcher: { type: "like", pattern: "EU%" },
	};
	const sql = queryModelToSql(q, "simple");
	expect(sql).toContain("/* filter not representable in minimal SQL:");
	expect(sql).not.toContain("WHERE");
});

test("disabled filters are stripped before translation", () => {
	const q = model();
	q.filter = {
		type: "and",
		filters: [
			{ type: "column", column: "country", valueMatcher: "France" },
			// `disabled: true` — must not reach the SQL output
			{ type: "column", column: "ccy", valueMatcher: "EUR", disabled: true },
		],
	};
	const sql = queryModelToSql(q, "simple");
	expect(sql).toContain(`WHERE "country" = 'France'`);
	expect(sql).not.toContain("EUR");
});

test("a fully-disabled filter yields no WHERE clause at all", () => {
	const q = model();
	q.filter = { type: "column", column: "country", valueMatcher: "France", disabled: true };
	const sql = queryModelToSql(q, "simple");
	expect(sql).not.toContain("WHERE");
});

test("numeric and boolean literals are emitted bare, not quoted", () => {
	const q = model();
	q.filter = {
		type: "and",
		filters: [
			{ type: "column", column: "qty", valueMatcher: 42 },
			{ type: "column", column: "flag", valueMatcher: true },
		],
	};
	const sql = queryModelToSql(q, "simple");
	expect(sql).toContain(`"qty" = 42`);
	expect(sql).toContain(`"flag" = true`);
});

test("single-quotes in string literals are doubled per ANSI SQL", () => {
	const q = model();
	q.filter = { type: "column", column: "city", valueMatcher: "O'Brien" };
	const sql = queryModelToSql(q, "simple");
	expect(sql).toContain(`WHERE "city" = 'O''Brien'`);
});

test("double-quotes in identifiers are doubled", () => {
	const q = model();
	q.selectedMeasures['weird"name'] = true;
	q.selectedColumnsOrdered.push('col"umn');
	q.filter = { type: "column", column: 'col"umn', valueMatcher: "v" };
	const sql = queryModelToSql(q, 'cub"e');
	expect(sql).toContain('"weird""name"');
	expect(sql).toContain('"col""umn"');
	expect(sql).toContain('FROM "cub""e"');
});

test("withStarColumns wrap the matching column in ROLLUP inside GROUP BY", () => {
	const q = model();
	q.selectedColumnsOrdered.push("city");
	q.withStarColumns.city = true;
	const sql = queryModelToSql(q, "simple");
	expect(sql).toContain('GROUP BY ROLLUP("city")');
});

test("withStarColumns mixes plain + ROLLUP in a single GROUP BY", () => {
	const q = model();
	q.selectedColumnsOrdered.push("city", "country");
	q.withStarColumns.country = true;
	const sql = queryModelToSql(q, "simple");
	expect(sql).toContain('GROUP BY "city", ROLLUP("country")');
});

test("withStarColumns=false (falsy) is equivalent to not set", () => {
	const q = model();
	q.selectedColumnsOrdered.push("city");
	q.withStarColumns.city = false;
	const sql = queryModelToSql(q, "simple");
	expect(sql).toContain('GROUP BY "city"');
	expect(sql).not.toContain("ROLLUP");
});

test("no measures, no columns, no filter, empty cube name: minimal SELECT * FROM ", () => {
	const sql = queryModelToSql(model(), "");
	expect(sql).toEqual(["SELECT *", 'FROM ""'].join("\n"));
});

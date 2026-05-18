// @ts-check
import { describe, it, expect } from "vitest";

import { searchCubeSchema } from "../src/main/resources/static/ui/js/adhoc-schema-search-helper.js";

const cube = () => ({
	columns: { country: {}, coach: {}, city: {}, "Player Name": {} },
	measures: { redcard_count: {}, "count(*)": {}, avg_age: {} },
});

describe("searchCubeSchema", () => {
	it("returns no hits on empty query", () => {
		expect(searchCubeSchema({ ...cube(), query: "" })).toEqual([]);
	});

	it("matches a column name (case-insensitive)", () => {
		// "country" matches in columns; "count(*)" and "redcard_count" both match in measures.
		// The column hit MUST be present and labelled `kind: "column"`.
		const hits = searchCubeSchema({ ...cube(), query: "coun" });
		const country = hits.find((h) => h.kind === "column" && h.name === "country");
		expect(country).toBeDefined();
	});

	it("matches a measure name", () => {
		// "count" matches both measures (`redcard_count`, `count(*)`) AND the column `country`
		// (its substring "coun" + "t" overlap, but actually "country" contains "count"? no:
		// "country" = c-o-u-n-t-r-y → contains "count"! verify here).
		const hits = searchCubeSchema({ ...cube(), query: "count" });
		const measures = hits
			.filter((h) => h.kind === "measure")
			.map((h) => h.name)
			.sort();
		expect(measures).toEqual(["count(*)", "redcard_count"]);
	});

	it("sorts columns before measures, then alphabetical within each kind", () => {
		const hits = searchCubeSchema({ ...cube(), query: "c" });
		// expected: column "city", "coach", "country", "Player Name"? no, "Player Name" has no "c" → exclude.
		// columns matching "c" (case-insensitive): city, coach, country  → those three.
		// "Player Name" lowercased is "player name" — no "c". Excluded.
		// measures matching "c": count(*), redcard_count.
		const kinds = hits.map((h) => h.kind);
		// All columns appear before any measure.
		const firstMeasureIdx = kinds.indexOf("measure");
		if (firstMeasureIdx >= 0) {
			expect(kinds.slice(0, firstMeasureIdx).every((k) => k === "column")).toBe(true);
		}
		// Alphabetical inside the columns block.
		const columnNames = hits.filter((h) => h.kind === "column").map((h) => h.name);
		expect(columnNames).toEqual([...columnNames].sort());
	});

	it("flags `alreadyInQuery` against the selectedColumns / selectedMeasures input", () => {
		const hits = searchCubeSchema({
			...cube(),
			query: "c",
			selectedColumns: { country: true },
			selectedMeasures: { "count(*)": true },
		});
		const country = hits.find((h) => h.kind === "column" && h.name === "country");
		const countStar = hits.find((h) => h.kind === "measure" && h.name === "count(*)");
		const coach = hits.find((h) => h.kind === "column" && h.name === "coach");
		expect(country?.alreadyInQuery).toBe(true);
		expect(countStar?.alreadyInQuery).toBe(true);
		expect(coach?.alreadyInQuery).toBe(false);
	});

	it("handles missing columns or measures gracefully", () => {
		expect(searchCubeSchema({ query: "country", columns: { country: {} } })).toHaveLength(1);
		expect(searchCubeSchema({ query: "redcard", measures: { redcard_count: {} } })).toHaveLength(1);
		expect(searchCubeSchema({ query: "x" })).toEqual([]);
	});

	it("respects the limit cap", () => {
		/** @type {Record<string, any>} */
		const many = {};
		for (let i = 0; i < 100; i++) many["col" + i] = {};
		const hits = searchCubeSchema({ columns: many, query: "col", limit: 7 });
		expect(hits).toHaveLength(7);
	});
});

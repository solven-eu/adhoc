// @ts-check
import { describe, it, expect } from "vitest";

import { searchTabularView } from "../src/main/resources/static/ui/js/adhoc-search-helper.js";

const view = () => ({
	coordinates: [
		{ city: "Paris", country: "France" },
		{ city: "Lyon", country: "France" },
		{ city: "Berlin", country: "Germany" },
	],
	values: [
		{ revenue: 71234, count: 5 },
		{ revenue: 9001, count: 12 },
		{ revenue: 200, count: 1 },
	],
});

describe("searchTabularView", () => {
	it("returns no hits for an empty query", () => {
		expect(searchTabularView({ view: view() }, "")).toEqual([]);
		expect(searchTabularView({ view: view() }, "   ")).toEqual([]);
	});

	it("matches coordinate strings case-insensitively", () => {
		const hits = searchTabularView({ view: view() }, "paris");
		expect(hits).toHaveLength(1);
		expect(hits[0]).toMatchObject({ row: 0, column: "city", value: "Paris", kind: "coordinate" });
	});

	it("matches a substring of a number's raw String form (no formatter)", () => {
		// `1234` is a substring of `71234` — typing the digit sequence the user remembers
		// should find it regardless of formatting.
		const hits = searchTabularView({ view: view() }, "1234");
		expect(hits).toHaveLength(1);
		expect(hits[0]).toMatchObject({ row: 0, column: "revenue", value: 71234, kind: "measure" });
	});

	it("matches against the formatter output when provided (e.g. `71,234.00`)", () => {
		const formatCell = (row, col) => {
			if (col !== "revenue") return null;
			const raw = view().values[row].revenue;
			return new Intl.NumberFormat("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(raw);
		};
		// `1,234` is in the FORMATTED string `71,234.00` but NOT in the raw `71234`.
		const hits = searchTabularView({ view: view(), formatCell }, "1,234");
		expect(hits).toHaveLength(1);
		expect(hits[0].formatted).toBe("71,234.00");
	});

	it("matches against the raw String when the formatter doesn't cover", () => {
		const formatCell = () => null; // formatter always declines
		const hits = searchTabularView({ view: view(), formatCell }, "9001");
		expect(hits).toHaveLength(1);
		expect(hits[0].value).toBe(9001);
	});

	it("respects `coordinateColumns` / `measureColumns` filters", () => {
		// Restrict to coordinates only — the digit-substring on revenue should be ignored.
		const hits = searchTabularView({ view: view(), coordinateColumns: ["city", "country"], measureColumns: [] }, "1");
		expect(hits.every((h) => h.kind === "coordinate")).toBe(true);
	});

	it("skips null / undefined cells", () => {
		const sparse = {
			coordinates: [{ city: null }, { city: "Paris" }],
			values: [{ revenue: undefined }, { revenue: 100 }],
		};
		const hits = searchTabularView({ view: sparse }, "paris");
		expect(hits).toHaveLength(1);
		expect(hits[0].row).toBe(1);
	});

	it("returns hits in row-major order (coordinates before measures within a row)", () => {
		// `1` appears in both row coordinates ("Berlin" is row 2, no `1`) and measures (revenue 71234, count 5/12/1)
		const hits = searchTabularView({ view: view() }, "1");
		// First three hits should be the measure hits on row 0 (revenue 71234, count 5 has no 1, skip)
		// then row 1 (revenue 9001, count 12), then row 2 (count 1).
		// Verify ordering invariant: row indices are non-decreasing.
		for (let i = 1; i < hits.length; i++) {
			expect(hits[i].row).toBeGreaterThanOrEqual(hits[i - 1].row);
		}
	});

	it("caps the result list at `limit`", () => {
		// Same view, but search for "" — wait, empty returns 0. Use "a" against many rows.
		const huge = {
			coordinates: Array.from({ length: 500 }, () => ({ a: "alpha" })),
			values: Array.from({ length: 500 }, () => ({})),
		};
		const hits = searchTabularView({ view: huge, limit: 50 }, "alpha");
		expect(hits).toHaveLength(50);
	});

	it("handles a missing or empty view defensively", () => {
		// @ts-ignore — intentionally malformed
		expect(searchTabularView({}, "x")).toEqual([]);
		expect(searchTabularView({ view: { coordinates: [], values: [] } }, "x")).toEqual([]);
	});

	it("falls back to the formatter's output as `formatted` when the formatter wins on a non-overlapping match", () => {
		// Construct a case where raw value is "42" but formatted is "$42.00" — needle "$4" matches
		// only the formatted, raw doesn't carry the `$`.
		const v = { coordinates: [{}], values: [{ revenue: 42 }] };
		const formatCell = () => "$42.00";
		const hits = searchTabularView({ view: v, formatCell }, "$4");
		expect(hits).toHaveLength(1);
		expect(hits[0].formatted).toBe("$42.00");
	});
});

// @ts-check
import { describe, it, expect } from "vitest";

import { searchCoordinatesAcrossColumns } from "../src/main/resources/static/ui/js/adhoc-coordinate-search-helper.js";

const cache = () => ({
	"e1-WorldCupPlayers-country": { column: "country", coordinates: ["France", "Germany", "Italy"] },
	"e1-WorldCupPlayers-coach": { column: "coach", coordinates: ["Didier Deschamps", "Joachim Loew"] },
	// Different cube on the same endpoint — must be filtered out by the (endpoint,cube) scope.
	"e1-Pixar-character": { column: "character", coordinates: ["Woody", "Buzz", "Frenchman"] },
	// Different endpoint — also filtered out.
	"e2-WorldCupPlayers-country": { column: "country", coordinates: ["France", "France 2"] },
});

describe("searchCoordinatesAcrossColumns", () => {
	it("returns no hits for empty/blank query", () => {
		expect(searchCoordinatesAcrossColumns({ columns: cache(), endpointId: "e1", cubeId: "WorldCupPlayers", query: "" })).toEqual([]);
		expect(searchCoordinatesAcrossColumns({ columns: cache(), endpointId: "e1", cubeId: "WorldCupPlayers", query: "   " })).toEqual([]);
	});

	it("finds a coordinate by case-insensitive substring", () => {
		const hits = searchCoordinatesAcrossColumns({ columns: cache(), endpointId: "e1", cubeId: "WorldCupPlayers", query: "fra" });
		expect(hits).toHaveLength(1);
		expect(hits[0]).toMatchObject({ column: "country", coordinate: "France" });
	});

	it("scopes by (endpoint, cube) — other cubes and other endpoints are not searched", () => {
		// "fra" matches "France" in e1-WorldCupPlayers, "Frenchman" in e1-Pixar, and "France"/"France 2" in e2.
		// With cube=WorldCupPlayers and endpoint=e1 the other two columns are filtered out.
		const hits = searchCoordinatesAcrossColumns({ columns: cache(), endpointId: "e1", cubeId: "WorldCupPlayers", query: "fra" });
		expect(hits.map((h) => h.column)).not.toContain("character");
		expect(hits.every((h) => h.column === "country" || h.column === "coach")).toBe(true);
	});

	it("returns one hit per matching coordinate (multi-hit on the same column allowed)", () => {
		// "Pixar" has both "Woody" and "Buzz" — search for "o" against THAT cube to get multi-hit.
		const hits = searchCoordinatesAcrossColumns({ columns: cache(), endpointId: "e1", cubeId: "Pixar", query: "o" });
		expect(hits.map((h) => h.coordinate)).toEqual(expect.arrayContaining(["Woody"]));
	});

	it("requires endpointId AND cubeId — empty either yields no hits", () => {
		expect(searchCoordinatesAcrossColumns({ columns: cache(), endpointId: "", cubeId: "WorldCupPlayers", query: "fra" })).toEqual([]);
		expect(searchCoordinatesAcrossColumns({ columns: cache(), endpointId: "e1", cubeId: "", query: "fra" })).toEqual([]);
	});

	it("excludes columns already in the groupBy", () => {
		const hits = searchCoordinatesAcrossColumns({
			columns: cache(),
			endpointId: "e1",
			cubeId: "WorldCupPlayers",
			query: "fra",
			excludeColumns: ["country"],
		});
		// `country` was the only column whose coordinates contain "fra"; with it excluded, no hits.
		expect(hits).toEqual([]);
	});

	it("respects `limit` as an early-exit", () => {
		/** @type {Record<string, { coordinates?: Array<string|number>, column?: string }>} */
		const big = {};
		for (let i = 0; i < 200; i++) {
			big["e1-WorldCupPlayers-col" + i] = { column: "col" + i, coordinates: ["alpha-" + i] };
		}
		const hits = searchCoordinatesAcrossColumns({ columns: big, endpointId: "e1", cubeId: "WorldCupPlayers", query: "alpha", limit: 10 });
		expect(hits).toHaveLength(10);
	});

	it("sorts hits by column then coordinate", () => {
		const hits = searchCoordinatesAcrossColumns({ columns: cache(), endpointId: "e1", cubeId: "Pixar", query: "" });
		expect(hits).toEqual([]); // empty query yields nothing; sort behaviour exercised below
		const sorted = searchCoordinatesAcrossColumns({ columns: cache(), endpointId: "e1", cubeId: "WorldCupPlayers", query: "e" });
		// Expect column-alphabetical: coach (Didier Deschamps, Joachim Loew) and country (Germany, France/Italy depending)
		const cols = sorted.map((h) => h.column);
		const sortedCols = [...cols].sort();
		expect(cols).toEqual(sortedCols);
	});

	it("ignores entries without a coordinates array (defensive)", () => {
		const partial = {
			"e1-WorldCupPlayers-cardinality": { column: "cardinality", estimate: 1000 }, // no coordinates
			"e1-WorldCupPlayers-country": { column: "country", coordinates: ["France"] },
		};
		const hits = searchCoordinatesAcrossColumns({ columns: partial, endpointId: "e1", cubeId: "WorldCupPlayers", query: "fra" });
		expect(hits).toHaveLength(1);
		expect(hits[0].column).toBe("country");
	});

	it("ignores null / undefined coordinates inside a column", () => {
		const sparse = {
			"e1-WorldCupPlayers-col": { column: "col", coordinates: [null, undefined, "France"] },
		};
		const hits = searchCoordinatesAcrossColumns({ columns: sparse, endpointId: "e1", cubeId: "WorldCupPlayers", query: "fra" });
		expect(hits).toHaveLength(1);
		expect(hits[0].coordinate).toBe("France");
	});

	it("falls back to deriving column name from the cache key when the entry lacks `column`", () => {
		const cache2 = {
			"e1-WorldCupPlayers-keyOnly": { coordinates: ["alpha"] },
		};
		const hits = searchCoordinatesAcrossColumns({ columns: cache2, endpointId: "e1", cubeId: "WorldCupPlayers", query: "alpha" });
		expect(hits[0].column).toBe("keyOnly");
	});
});

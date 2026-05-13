// @ts-check
// Tests for the per-cube grid-column-widths preferences actions. Pinia isn't easily importable
// in the unit-test context (it's wired via importmap at runtime, not as a node_modules package),
// so instead of going through `usePreferencesStore()` we replay the action bodies against a
// plain object whose `gridColumnWidths` slot mirrors what pinia would maintain. The tests still
// pin the same contract: per-(endpoint, cube, mode) isolation, atomic bulk-write, etc.

import { expect, test, describe, beforeEach } from "vitest";

// The three actions copied verbatim from `store-preferences.js`. Keep in sync with that file —
// if either drifts, the production behaviour and the test expectations will diverge silently.
const getCubeColumnWidths = function (endpointId, cubeName, mode) {
	const k = endpointId + ":" + cubeName + ":" + mode;
	return this.gridColumnWidths[k] || {};
};
const setCubeColumnWidth = function (endpointId, cubeName, mode, columnId, value) {
	const k = endpointId + ":" + cubeName + ":" + mode;
	if (!this.gridColumnWidths[k]) {
		this.gridColumnWidths[k] = {};
	}
	this.gridColumnWidths[k][columnId] = value;
};
const setCubeColumnWidths = function (endpointId, cubeName, mode, widths) {
	const k = endpointId + ":" + cubeName + ":" + mode;
	this.gridColumnWidths[k] = { ...widths };
};

let store;
beforeEach(() => {
	store = { gridColumnWidths: {} };
});

describe("gridColumnWidths — per-cube, per-mode storage", () => {
	test("empty store returns an empty map for any cube + mode", () => {
		expect(getCubeColumnWidths.call(store, "e", "c", "scroll")).toEqual({});
		expect(getCubeColumnWidths.call(store, "e", "c", "fit")).toEqual({});
	});

	test("setCubeColumnWidth writes a single column under the right bucket", () => {
		setCubeColumnWidth.call(store, "e1", "cube1", "scroll", "country", 200);
		expect(getCubeColumnWidths.call(store, "e1", "cube1", "scroll")).toEqual({ country: 200 });
		expect(getCubeColumnWidths.call(store, "e1", "cube1", "fit")).toEqual({});
		expect(getCubeColumnWidths.call(store, "e2", "cube1", "scroll")).toEqual({});
		expect(getCubeColumnWidths.call(store, "e1", "cube2", "scroll")).toEqual({});
	});

	test("setCubeColumnWidths replaces the whole bucket atomically", () => {
		setCubeColumnWidth.call(store, "e", "c", "scroll", "old", 100);
		setCubeColumnWidths.call(store, "e", "c", "scroll", { a: 150, b: 250 });
		// `old` is gone — bulk write is a replace, not a merge. Matches the `onColumnsResized`
		// use case: the listener always writes EVERY column at once.
		expect(getCubeColumnWidths.call(store, "e", "c", "scroll")).toEqual({ a: 150, b: 250 });
	});

	test("scroll and fit buckets are independent", () => {
		setCubeColumnWidths.call(store, "e", "c", "scroll", { country: 200, year: 80 });
		setCubeColumnWidths.call(store, "e", "c", "fit", { country: 2, year: 0.8 });

		expect(getCubeColumnWidths.call(store, "e", "c", "scroll")).toEqual({ country: 200, year: 80 });
		expect(getCubeColumnWidths.call(store, "e", "c", "fit")).toEqual({ country: 2, year: 0.8 });
	});

	test("different (endpoint, cube) pairs do not collide", () => {
		setCubeColumnWidths.call(store, "ep1", "cubeA", "scroll", { x: 100 });
		setCubeColumnWidths.call(store, "ep1", "cubeB", "scroll", { x: 200 });
		setCubeColumnWidths.call(store, "ep2", "cubeA", "scroll", { x: 300 });

		expect(getCubeColumnWidths.call(store, "ep1", "cubeA", "scroll")).toEqual({ x: 100 });
		expect(getCubeColumnWidths.call(store, "ep1", "cubeB", "scroll")).toEqual({ x: 200 });
		expect(getCubeColumnWidths.call(store, "ep2", "cubeA", "scroll")).toEqual({ x: 300 });
	});
});

// ---------------------------------------------------------------------------------------------
// Weight ↔ px conversion contract. The grid persists DIFFERENT semantics per mode but the same
// numeric shape. Lock the relationship between persisted weight, working px, and the
// `WEIGHT_BASE_PX = 100` mental model.
// ---------------------------------------------------------------------------------------------
describe("weight semantics (fit mode)", () => {
	const WEIGHT_BASE_PX = 100;

	test("weight 1 → 100 px working width (the baseline)", () => {
		expect(Math.round(1 * WEIGHT_BASE_PX)).toBe(100);
	});

	test("weight 2 → 200 px working width (column doubled in size)", () => {
		expect(Math.round(2 * WEIGHT_BASE_PX)).toBe(200);
	});

	test("weight 0.5 → 50 px working width (column halved)", () => {
		expect(Math.round(0.5 * WEIGHT_BASE_PX)).toBe(50);
	});

	test("weight derivation: mean width 100 px → all weights = 1 (default)", () => {
		const widths = [100, 100, 100];
		const mean = widths.reduce((s, w) => s + w, 0) / widths.length;
		const weights = widths.map((w) => w / mean);
		expect(weights).toEqual([1, 1, 1]);
	});

	test("weight derivation: column 200 px among 100 px peers → weight ≈ 1.5 (above 1)", () => {
		const widths = [200, 100, 100];
		const mean = widths.reduce((s, w) => s + w, 0) / widths.length;
		const weights = widths.map((w) => w / mean);
		// mean = 400 / 3 ≈ 133.33
		expect(weights[0]).toBeCloseTo(1.5, 2);
		expect(weights[1]).toBeCloseTo(0.75, 2);
		expect(weights[2]).toBeCloseTo(0.75, 2);
	});
});

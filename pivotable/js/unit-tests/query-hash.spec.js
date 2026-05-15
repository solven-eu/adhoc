// @ts-check
import { expect, test } from "vitest";

//import lodashEs from "https://cdn.jsdelivr.net/npm/lodash-es@4.17.21/+esm";

import queryHelper from "@/js/adhoc-query-helper.js";

// https://vitest.dev/api/expect.html
test("loadQueryModelFromHash - undefined hash", () => {
	const reloadedQueryModel = queryHelper.makeQueryModel();
	queryHelper.hashToQueryModel(undefined, reloadedQueryModel);

	expect(reloadedQueryModel.selectedColumns).toEqual({});
	expect(reloadedQueryModel.selectedColumnsOrdered).toEqual([]);
});

// Back/forward navigation semantics: decoding a hash must REPLACE the current
// queryModel state, not be additive. Otherwise clicking back after adding a
// column would leave the added column stuck — the exact bug this guards against.
test("loadQueryModelFromHash - replaces prior state (browser back/forward)", () => {
	// Simulate a "prior view" that the user later edited.
	const original = queryHelper.makeQueryModel();
	original.selectedColumns.c1 = true;
	original.selectedColumnsOrdered.push("c1");
	original.selectedMeasures.m1 = true;

	const hashForOriginal = queryHelper.queryModelToHash(undefined, original);

	// Current state after edits: the user added c2 and m2, kept c1 and m1.
	const current = queryHelper.makeQueryModel();
	current.selectedColumns.c1 = true;
	current.selectedColumns.c2 = true;
	current.selectedColumnsOrdered.push("c1", "c2");
	current.selectedMeasures.m1 = true;
	current.selectedMeasures.m2 = true;

	// Browser back: decode the original hash on top of the "current" queryModel.
	queryHelper.hashToQueryModel(decodeURIComponent(hashForOriginal), current);

	// c2 and m2 must be GONE — not merged in.
	expect(current.selectedColumns).toEqual({ c1: true });
	expect(current.selectedColumnsOrdered).toEqual(["c1"]);
	expect(current.selectedMeasures).toEqual({ m1: true });
});

test("loadQueryModelFromHash - from 2 columns", () => {
	const originalQueryModel = queryHelper.makeQueryModel();
	originalQueryModel.selectedColumns.c1 = true;
	originalQueryModel.selectedColumns.c2 = false;
	originalQueryModel.selectedColumnsOrdered.push("c1");

	const queryModel = originalQueryModel.copy();
	const newHash = queryHelper.queryModelToHash(undefined, queryModel);

	if (!newHash.startsWith("#")) {
		throw new Error("Should starts with '#'");
	}

	expect(newHash).toEqual(
		"#" + encodeURIComponent(JSON.stringify({ query: { columns: ["c1"], withStarColumns: {}, measures: [], filter: {}, customMarkers: {}, options: [] } })),
	);
	// ("#%7B%22query%22%3A%7B%22columns%22%3A%5B%5D%2C%22withStarColumns%22%3A%7B%7D%2C%22measures%22%3A%5B%5D%2C%22filter%22%3A%7B%7D%2C%22customMarkers%22%3A%7B%7D%2C%22options%22%3A%5B%5D%7D%7D");

	const reloadedQueryModel = queryHelper.makeQueryModel();
	queryHelper.hashToQueryModel(decodeURIComponent(newHash), reloadedQueryModel);

	expect(reloadedQueryModel.selectedColumns).toEqual({ c1: true });
	expect(reloadedQueryModel.selectedColumnsOrdered).toEqual(["c1"]);
});

// ---------------------------------------------------------------------------------------------
// readUrlHash — the authoritative URL-hash reader used by adhoc-query.js's hydration path.
//
// Regression: vue-router's `currentRoute.value.hash` can be stale on the remount-after-login
// path (we use `history.pushState` to update the URL on every model edit, which bypasses
// vue-router's internal state). `readUrlHash` reads `window.location.hash` directly — that's
// always in sync with the real URL.
// ---------------------------------------------------------------------------------------------
test("readUrlHash: returns '' when window.location.hash is empty", () => {
	expect(queryHelper.readUrlHash({ location: { hash: "" } })).toBe("");
});

test("readUrlHash: returns '' when there is no leading #", () => {
	// Browsers never produce this shape, but defending against bogus input is cheap.
	expect(queryHelper.readUrlHash({ location: { hash: "no-hash" } })).toBe("");
});

test("readUrlHash: returns '' when windowLike or its location is missing", () => {
	expect(queryHelper.readUrlHash(null)).toBe("");
	expect(queryHelper.readUrlHash(undefined)).toBe("");
	expect(queryHelper.readUrlHash({})).toBe("");
	expect(queryHelper.readUrlHash({ location: {} })).toBe("");
});

test("readUrlHash: decodes URL-encoded queryModel JSON", () => {
	// `window.location.hash` returns the URL-encoded form; we decode so hashToQueryModel
	// receives the same shape as vue-router's `currentRoute.value.hash` (which is already
	// decoded). Round-trip via the helper functions below to construct a realistic fixture.
	const original = queryHelper.makeQueryModel();
	original.selectedColumns["Position"] = true;
	original.selectedColumnsOrdered.push("Position");
	const encodedHash = queryHelper.queryModelToHash(undefined, original);
	// Sanity: queryModelToHash produces an encoded hash.
	expect(encodedHash).toMatch(/^#%7B/);

	const decoded = queryHelper.readUrlHash({ location: { hash: encodedHash } });
	// After decoding, the leading `#` is preserved and the rest is plain JSON.
	expect(decoded.startsWith("#{")).toBe(true);

	// Round-trip: decoded form is what hashToQueryModel expects — no exception, model restored.
	const restored = queryHelper.makeQueryModel();
	queryHelper.hashToQueryModel(decoded, restored);
	expect(restored.selectedColumns).toEqual({ Position: true });
	expect(restored.selectedColumnsOrdered).toEqual(["Position"]);
});

test("readUrlHash: falls back to raw on a malformed URI sequence", () => {
	// `decodeURIComponent` throws on a lone `%` — make sure the helper doesn't propagate that.
	const raw = "#oops%invalid";
	expect(queryHelper.readUrlHash({ location: { hash: raw } })).toBe(raw);
});

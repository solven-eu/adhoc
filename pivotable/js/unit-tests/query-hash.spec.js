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

	// `v: 1` is stamped on every emit — it's the breaking-change marker that lets future
	// clients identify legacy URL payloads. See `URL_HASH_VERSION` Javadoc for the bump policy.
	expect(newHash).toEqual(
		"#" +
			encodeURIComponent(
				JSON.stringify({
					v: 1,
					query: { columns: ["c1"], withStarColumns: {}, measures: [], filter: {}, customMarkers: {}, options: [] },
				}),
			),
	);

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

// ---------------------------------------------------------------------------------------------
// URL_HASH_VERSION — the breaking-change marker stamped on every emitted hash and consulted by
// `hashToQueryModel`. Three behaviours to guard against regression:
//   1. Stamp on emit: the current client always writes `v: URL_HASH_VERSION` at the outer level.
//   2. Tolerant read of legacy unversioned URLs: missing `v` is treated as the current version
//      so links produced before the marker was introduced still resolve.
//   3. Refusal to half-restore from a future shape: an unknown future `v` bails out, leaving
//      queryModel at defaults rather than silently importing a partially-understood payload.
// ---------------------------------------------------------------------------------------------
test("URL_HASH_VERSION: queryModelToHash stamps the current version", () => {
	const model = queryHelper.makeQueryModel();
	const hash = queryHelper.queryModelToHash(undefined, model);
	const decoded = JSON.parse(decodeURIComponent(hash.substring(1)));
	expect(decoded.v).toBe(queryHelper.URL_HASH_VERSION);
});

test("URL_HASH_VERSION: legacy unversioned URL is accepted as if v=current", () => {
	// A hash produced before the `v` marker was introduced — no `v` key at the outer object level.
	const legacy = "#" + encodeURIComponent(JSON.stringify({ query: { columns: ["legacyCol"], measures: [], filter: {}, customMarkers: {}, options: [] } }));
	const restored = queryHelper.makeQueryModel();
	queryHelper.hashToQueryModel(decodeURIComponent(legacy), restored);
	expect(restored.selectedColumns).toEqual({ legacyCol: true });
});

test("URL_HASH_VERSION: unknown future v leaves queryModel at defaults", () => {
	const future =
		"#" +
		encodeURIComponent(
			JSON.stringify({
				v: queryHelper.URL_HASH_VERSION + 99,
				// Even if `query` is syntactically valid for the current shape, we refuse to import
				// it — the future client may have changed semantics underneath that we can't see.
				query: { columns: ["someCol"], measures: [], filter: {}, customMarkers: {}, options: [] },
			}),
		);
	const restored = queryHelper.makeQueryModel();
	queryHelper.hashToQueryModel(decodeURIComponent(future), restored);
	expect(restored.selectedColumns).toEqual({});
});

test("URL_HASH_VERSION: legacy URL is auto-upgraded to versioned on next emit", () => {
	// Realistic round-trip: user opens a legacy link → query is restored → user edits something →
	// the new hash carries the current `v`. No explicit migration step needed.
	const legacy = "#" + encodeURIComponent(JSON.stringify({ query: { columns: ["a"], measures: [], filter: {}, customMarkers: {}, options: [] } }));
	const restored = queryHelper.makeQueryModel();
	queryHelper.hashToQueryModel(decodeURIComponent(legacy), restored);
	const reemitted = queryHelper.queryModelToHash(decodeURIComponent(legacy), restored);
	const decoded = JSON.parse(decodeURIComponent(reemitted.substring(1)));
	expect(decoded.v).toBe(queryHelper.URL_HASH_VERSION);
});

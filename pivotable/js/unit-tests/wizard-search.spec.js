// @ts-check
import { expect, test, describe } from "vitest";

import wizardHelper from "@/js/adhoc-query-wizard-helper.js";

// Convenience: a minimal searchOptions object that mirrors what AdhocQueryWizardSearch maintains.
const opts = (overrides = {}) => ({
	text: "",
	caseSensitive: false,
	throughJson: false,
	filterQueried: false,
	tags: [],
	...overrides,
});

// ---------------------------------------------------------------------------------------------
// scoreSingle — the waterfall itself.
// ---------------------------------------------------------------------------------------------
describe("scoreSingle — waterfall of match rules", () => {
	test("exact match → 100", () => {
		expect(wizardHelper.scoreSingle("Currency", "Currency", false)).toBe(100);
	});

	test("equals-ignore-case → 90", () => {
		expect(wizardHelper.scoreSingle("Currency", "currency", false)).toBe(90);
	});

	test("contains (case-sensitive) → 80", () => {
		expect(wizardHelper.scoreSingle("LocalCurrency", "Currency", false)).toBe(80);
	});

	test("contains-ignore-case → 70", () => {
		expect(wizardHelper.scoreSingle("LocalCurrency", "currency", false)).toBe(70);
	});

	test("subsequence (case-sensitive) → 60", () => {
		// 'ad' as ordered fragments of 'abcd' — both letters appear in order.
		expect(wizardHelper.scoreSingle("abcd", "ad", false)).toBe(60);
	});

	test("subsequence-ignore-case → 50 — the canonical 'ccy → Currency' case", () => {
		expect(wizardHelper.scoreSingle("Currency", "ccy", false)).toBe(50);
	});

	test("no match → 0", () => {
		expect(wizardHelper.scoreSingle("Currency", "xyz", false)).toBe(0);
	});

	test("empty needle → 100 (everything matches at the top tier)", () => {
		expect(wizardHelper.scoreSingle("anything", "", false)).toBe(100);
	});

	test("caseSensitiveOnly drops every case-insensitive tier", () => {
		// With caseSensitiveOnly=true, 'currency' against 'Currency' must NOT promote to 90.
		expect(wizardHelper.scoreSingle("Currency", "currency", true)).toBe(0);
		// But 'Currency' against 'Currency' still scores 100.
		expect(wizardHelper.scoreSingle("Currency", "Currency", true)).toBe(100);
	});

	test("subsequence is in-order, not just set-membership", () => {
		// 'da' is NOT a subsequence of 'abcd' (d appears before a in the search).
		expect(wizardHelper.scoreSingle("abcd", "da", false)).toBe(0);
	});
});

// ---------------------------------------------------------------------------------------------
// isSubsequence — the building block for the bottom two tiers.
// ---------------------------------------------------------------------------------------------
describe("isSubsequence", () => {
	test("true on perfect contiguous match", () => {
		expect(wizardHelper.isSubsequence("abcd", "abcd")).toBe(true);
	});
	test("true on ordered non-contiguous fragments (case-matched)", () => {
		// isSubsequence is case-sensitive; the wizard's case-insensitive tier lower-cases both sides first.
		expect(wizardHelper.isSubsequence("currency", "ccy")).toBe(true);
		// 'Ccy' as a subsequence of 'Currency': C at 0, c at 6, y at 7.
		expect(wizardHelper.isSubsequence("Currency", "Ccy")).toBe(true);
		// But lowercase 'ccy' against the same string is NOT a subsequence (only one lowercase c).
		expect(wizardHelper.isSubsequence("Currency", "ccy")).toBe(false);
	});
	test("false on out-of-order fragments", () => {
		expect(wizardHelper.isSubsequence("Currency", "ycc")).toBe(false);
	});
	test("empty needle is a subsequence of anything", () => {
		expect(wizardHelper.isSubsequence("anything", "")).toBe(true);
	});
});

// ---------------------------------------------------------------------------------------------
// filtered — full integration: tag filter + waterfall scoring + sort + drop-tags fallback.
// ---------------------------------------------------------------------------------------------
describe("filtered — waterfall ranking", () => {
	const columns = {
		Currency: { tags: ["core"], type: "varchar" },
		LocalCurrency: { tags: ["core"], type: "varchar" },
		country: { tags: [], type: "varchar" },
		ccy_rate: { tags: [], type: "double" },
	};

	test("empty search yields every entry, ranked alphabetically at score 100", () => {
		const out = wizardHelper.filtered(opts(), columns, {});
		expect(out.map((c) => c.key)).toEqual(["ccy_rate", "country", "Currency", "LocalCurrency"]);
		// Every row at the top tier — no score badge in the UI.
		expect(out.every((c) => c._matchScore === 100)).toBe(true);
	});

	test("'ccy' surfaces Currency via the subsequence tier", () => {
		const out = wizardHelper.filtered(opts({ text: "ccy" }), columns, {});
		const byKey = Object.fromEntries(out.map((c) => [c.key, c._matchScore]));

		// 'ccy_rate' contains 'ccy' → score 80 (tier 3, contains case-sensitive).
		expect(byKey.ccy_rate).toBe(80);
		// 'LocalCurrency' has 'ccy' as a case-sensitive subsequence (c at 2, c at 11, y at 12) → 60.
		expect(byKey.LocalCurrency).toBe(60);
		// 'Currency' has 'ccy' as a case-INsensitive subsequence only — single uppercase C requires
		// the lowercased-match path → tier 6, score 50.
		expect(byKey.Currency).toBe(50);
		// 'country' has no 'ccy' subsequence at all — excluded.
		expect(byKey.country).toBeUndefined();

		// Sort: highest score first.
		expect(out.map((c) => c.key)).toEqual(["ccy_rate", "LocalCurrency", "Currency"]);
	});

	test("waterfall tiers rank exact > equals-ignore-case > contains > contains-ignore-case", () => {
		// Build a controlled corpus where each tier is represented exactly once.
		const corpus = {
			Currency: { tags: [], type: "varchar" }, // exact when search="Currency"
			currency: { tags: [], type: "varchar" }, // equals-ignore-case
			LocalCurrency: { tags: [], type: "varchar" }, // contains
			localcurrency: { tags: [], type: "varchar" }, // contains-ignore-case
		};
		const out = wizardHelper.filtered(opts({ text: "Currency" }), corpus, {});
		const byKey = Object.fromEntries(out.map((c) => [c.key, c._matchScore]));
		expect(byKey.Currency).toBe(100);
		expect(byKey.currency).toBe(90);
		expect(byKey.LocalCurrency).toBe(80);
		expect(byKey.localcurrency).toBe(70);
		// Sort respects the ranking.
		expect(out.map((c) => c.key)).toEqual(["Currency", "currency", "LocalCurrency", "localcurrency"]);
	});

	test("caseSensitive toggle removes every case-insensitive tier from results", () => {
		const corpus = {
			Currency: { tags: [], type: "varchar" },
			currency: { tags: [], type: "varchar" },
			localcurrency: { tags: [], type: "varchar" },
		};
		const out = wizardHelper.filtered(opts({ text: "Currency", caseSensitive: true }), corpus, {});
		const keys = out.map((c) => c.key);
		// Only the exact case-sensitive match makes it through.
		expect(keys).toEqual(["Currency"]);
	});

	test("tag filter excludes non-matching rows in the strict pass", () => {
		const out = wizardHelper.filtered(opts({ text: "", tags: ["core"] }), columns, {});
		expect(out.map((c) => c.key).sort()).toEqual(["Currency", "LocalCurrency"]);
		// None of these are tag-bypassed — they all carry the required tag.
		expect(out.every((c) => !c._matchTagsBypassed)).toBe(true);
	});

	test("drop-tags fallback surfaces matches that violate the tag filter, at a discounted score, with the bypass flag", () => {
		// Strict pass: nothing in the `xyz` tag matches the text "ccy".
		// Fallback: Currency / LocalCurrency / ccy_rate all match the text, but none carry the `xyz` tag.
		const out = wizardHelper.filtered(opts({ text: "ccy", tags: ["xyz"] }), columns, {});

		// We get matches via the fallback.
		expect(out.length).toBeGreaterThan(0);
		expect(out.every((c) => c._matchTagsBypassed === true)).toBe(true);

		// Each score is discounted by TAG_FALLBACK_DISCOUNT from its strict-pass score.
		// 'ccy_rate' would score 80 strict, so 80 - 30 = 50 in the fallback.
		const byKey = Object.fromEntries(out.map((c) => [c.key, c._matchScore]));
		expect(byKey.ccy_rate).toBe(80 - wizardHelper.TAG_FALLBACK_DISCOUNT);
		// 'Currency' would score 50 strict (subsequence-i), so 50 - 30 = 20.
		expect(byKey.Currency).toBe(50 - wizardHelper.TAG_FALLBACK_DISCOUNT);
	});

	test("drop-tags fallback does NOT trigger when there is no search text", () => {
		// User just toggled a tag chip with empty search — empty result is the right answer.
		const out = wizardHelper.filtered(opts({ text: "", tags: ["xyz"] }), columns, {});
		expect(out).toEqual([]);
	});

	test("drop-tags fallback does NOT trigger when the strict pass found something", () => {
		// 'core'-tagged rows DO match 'curr' strictly — no fallback needed.
		const out = wizardHelper.filtered(opts({ text: "curr", tags: ["core"] }), columns, {});
		expect(out.length).toBeGreaterThan(0);
		expect(out.every((c) => !c._matchTagsBypassed)).toBe(true);
	});

	test("subsequence is order-sensitive in filtered() too", () => {
		// 'rcy' is a subsequence of 'Currency' (positions 3, 6, 7).
		const out1 = wizardHelper.filtered(opts({ text: "rcy" }), columns, {});
		expect(out1.map((c) => c.key)).toContain("Currency");

		// 'ycr' is NOT a subsequence of 'Currency' (y appears after r/c).
		const out2 = wizardHelper.filtered(opts({ text: "ycr" }), columns, {});
		expect(out2.map((c) => c.key)).not.toContain("Currency");
	});
});

// ---------------------------------------------------------------------------------------------
// historyScores — personal-history secondary sort. Three invariants to pin down so the feature
// doesn't drift into making search worse:
//   1. When two items share a text-match tier, the one with the higher history score wins.
//   2. History NEVER pushes a worse-tier text match above a better-tier one.
//   3. Absent / undefined / empty history degrades cleanly to the previous (matchScore, alpha)
//      ordering — no regression for users with an empty store.
// ---------------------------------------------------------------------------------------------
describe("filtered — historyScores secondary sort", () => {
	const columns = {
		Currency: { tags: [], type: "varchar" },
		LocalCurrency: { tags: [], type: "varchar" },
		country: { tags: [], type: "varchar" },
		ccy_rate: { tags: [], type: "double" },
	};

	test("no historyScores → previous alpha ordering inside the score-100 tier", () => {
		const out = wizardHelper.filtered(opts(), columns, {});
		expect(out.map((c) => c.key)).toEqual(["ccy_rate", "country", "Currency", "LocalCurrency"]);
	});

	test("history is IGNORED in the unfiltered (no-search) view — alpha stays stable", () => {
		// Design rule: the default catalogue view must be lexicographical. Users build a mental
		// map of where each column / measure lives in the alphabet; reordering it under their
		// finger on every page load (as personal-history scoring would) erodes that mental map.
		// History affinity is still surfaced via the per-row badge below — it just doesn't move
		// the rows in the absence of search.
		const history = new Map([
			["country", 999], // huge usage signal
			["LocalCurrency", 500],
		]);
		const out = wizardHelper.filtered(opts({ historyScores: history }), columns, {});
		// Pure alpha — same as the "no historyScores" case above.
		expect(out.map((c) => c.key)).toEqual(["ccy_rate", "country", "Currency", "LocalCurrency"]);
		// But the per-row history score IS stamped so the row component can render the
		// "used before" affordance regardless of sort order — discoverability over rearrangement.
		expect(out.find((c) => c.key === "country")._historyScore).toBe(999);
		expect(out.find((c) => c.key === "ccy_rate")._historyScore).toBe(0);
	});

	test("history breaks ties WHEN search is active", () => {
		// Two items share the contains tier (substring 'cur' present case-sensitively → score 80).
		// Without history, alpha would put currency_a first; history on currency_b flips the tier.
		const corpus = {
			currency_a: { tags: [], type: "varchar" },
			currency_b: { tags: [], type: "varchar" },
		};
		const history = new Map([["currency_b", 50]]);
		const out = wizardHelper.filtered(opts({ text: "cur", historyScores: history }), corpus, {});
		expect(out[0].key).toBe("currency_b");
		expect(out[1].key).toBe("currency_a");
	});

	test("history NEVER beats a stronger text-match", () => {
		// Search "Currency": Currency=100 (exact), LocalCurrency=80 (contains). Even if the user
		// has touched LocalCurrency 1000× and never touched Currency, the exact-match Currency
		// must still surface first — history is a TIE-BREAKER, not a tier-jumper.
		const history = new Map([["LocalCurrency", 1000]]);
		const out = wizardHelper.filtered(opts({ text: "Currency", historyScores: history }), columns, {});
		expect(out[0].key).toBe("Currency");
		// LocalCurrency still appears (it's in the contains tier), just below.
		expect(out.find((c) => c.key === "LocalCurrency")).toBeTruthy();
	});

	test("plain-object historyScores work the same as a Map (test convenience path)", () => {
		// With NO search, alpha order wins regardless of history. Smoke-test the plain-object
		// path: historyScoreOf must accept both — assert by inspecting the stamped score, not
		// the order, since the no-search alpha behaviour swallows the boost.
		const out = wizardHelper.filtered(opts({ historyScores: { country: 50 } }), columns, {});
		expect(out.find((c) => c.key === "country")._historyScore).toBe(50);
		// And with search ON, the plain-object boost participates in tie-breaking as expected.
		// Same tier shape as the "history breaks ties" test above.
		const out2 = wizardHelper.filtered(opts({ text: "cur", historyScores: { currency_b: 50 } }), { currency_a: {}, currency_b: {} }, {});
		expect(out2[0].key).toBe("currency_b");
	});

	test("an unknown name gets historyScore 0 — never crashes, never reorders", () => {
		const out = wizardHelper.filtered(opts({ historyScores: new Map([["does_not_exist", 999]]) }), columns, {});
		// Same as the no-history case — the unknown-name entry doesn't pollute output.
		expect(out.map((c) => c.key)).toEqual(["ccy_rate", "country", "Currency", "LocalCurrency"]);
	});
});

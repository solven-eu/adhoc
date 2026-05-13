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

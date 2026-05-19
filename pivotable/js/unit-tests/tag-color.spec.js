// @ts-check
import { describe, it, expect } from "vitest";

import { tagToHue, tagToStyle } from "../src/main/resources/static/ui/js/adhoc-tag-color.js";

describe("tagToHue", () => {
	it("is deterministic — same tag → same hue", () => {
		expect(tagToHue("kpi")).toBe(tagToHue("kpi"));
		expect(tagToHue("internal")).toBe(tagToHue("internal"));
	});

	it("returns a value in [0, 360)", () => {
		for (const t of ["kpi", "internal", "sports", "demo", "", "with space", "é"]) {
			const h = tagToHue(t);
			expect(h).toBeGreaterThanOrEqual(0);
			expect(h).toBeLessThan(360);
		}
	});

	it("distinguishes different tags (with extremely high probability)", () => {
		// Hash collisions on 360 buckets exist, but a handful of distinct tags should land on
		// at least 4 different hues — anything less suggests the hash collapsed.
		const hues = new Set(["alpha", "beta", "gamma", "delta", "epsilon", "zeta"].map(tagToHue));
		expect(hues.size).toBeGreaterThanOrEqual(4);
	});

	it("handles empty / non-string inputs without throwing", () => {
		expect(typeof tagToHue("")).toBe("number");
		// @ts-ignore intentionally invalid
		expect(typeof tagToHue(null)).toBe("number");
		// @ts-ignore intentionally invalid
		expect(typeof tagToHue(undefined)).toBe("number");
	});
});

describe("tagToStyle", () => {
	it("emits an HSL `background-color` plus white text by default", () => {
		const style = tagToStyle("kpi");
		expect(style).toMatch(/background-color: hsl\(\d+(\.\d+)?, \d+%, \d+%\);/);
		expect(style).toContain("color: white;");
	});

	it("honours an explicit override over the auto-computed colour", () => {
		expect(tagToStyle("kpi", "#1f6feb")).toBe("background-color: #1f6feb; color: white;");
		expect(tagToStyle("kpi", "rebeccapurple")).toBe("background-color: rebeccapurple; color: white;");
	});

	it("falls back to the auto path when the override is empty / nullish", () => {
		expect(tagToStyle("kpi", "")).toBe(tagToStyle("kpi"));
		expect(tagToStyle("kpi", undefined)).toBe(tagToStyle("kpi"));
		// @ts-ignore intentionally invalid
		expect(tagToStyle("kpi", null)).toBe(tagToStyle("kpi"));
	});
});

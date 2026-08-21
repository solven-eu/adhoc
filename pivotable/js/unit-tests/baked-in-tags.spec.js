import { describe, it, expect } from "vitest";

import {
	TAG_ESSENTIAL,
	TAG_HIDDEN,
	describeTag,
	hasTag,
	isExcludedAsHidden,
	collectCubeTags,
	defaultSelectedTags,
} from "../src/main/resources/static/ui/js/adhoc-baked-in-tags.js";

describe("describeTag", () => {
	it("describes the baked-in tags", () => {
		expect(describeTag(TAG_ESSENTIAL, null)).toContain("Useful in most cases");
		expect(describeTag("composite-full", null)).toContain("every sub-cube");
	});

	it("prefers a cube-declared description over the baked-in one", () => {
		expect(describeTag("meta", { meta: "Our own meaning" })).toBe("Our own meaning");
	});

	it("falls back to the baked-in description when the cube describes other tags", () => {
		expect(describeTag("meta", { risk: "…" })).toBe(describeTag("meta", null));
	});

	it("returns an empty string for an unknown tag, so the tooltip is simply absent", () => {
		expect(describeTag("pnl", null)).toBe("");
	});
});

describe("hasTag", () => {
	it("tolerates an item without tags, as an error column carries none", () => {
		expect(hasTag({}, TAG_HIDDEN)).toBe(false);
		expect(hasTag(null, TAG_HIDDEN)).toBe(false);
	});

	it("detects a carried tag", () => {
		expect(hasTag({ tags: ["a", TAG_HIDDEN] }, TAG_HIDDEN)).toBe(true);
	});
});

describe("isExcludedAsHidden", () => {
	it("excludes a hidden item by default", () => {
		expect(isExcludedAsHidden({ tags: [TAG_HIDDEN] }, [])).toBe(true);
	});

	it("keeps a hidden item once the hidden tag is explicitly selected", () => {
		expect(isExcludedAsHidden({ tags: [TAG_HIDDEN] }, [TAG_HIDDEN])).toBe(false);
	});

	it("never excludes an item which is not hidden", () => {
		expect(isExcludedAsHidden({ tags: ["essential"] }, [])).toBe(false);
		expect(isExcludedAsHidden({}, [])).toBe(false);
	});

	it("still excludes when other tags are selected", () => {
		expect(isExcludedAsHidden({ tags: [TAG_HIDDEN, "risk"] }, ["risk"])).toBe(true);
	});
});

describe("collectCubeTags", () => {
	it("unions measure and column tags", () => {
		const cube = {
			measures: { delta: { tags: ["greeks", "essential"] } },
			columns: { columns: { ccy: { tags: ["essential"] } } },
		};

		expect(collectCubeTags(cube).sort()).toEqual(["essential", "greeks"]);
	});

	it("tolerates a missing cube, missing measures and untagged columns", () => {
		expect(collectCubeTags(null)).toEqual([]);
		expect(collectCubeTags({})).toEqual([]);
		expect(collectCubeTags({ columns: { columns: { broken: {} } } })).toEqual([]);
	});
});

describe("defaultSelectedTags", () => {
	it("preselects essential when the cube declares it", () => {
		expect(defaultSelectedTags(["greeks", TAG_ESSENTIAL])).toEqual([TAG_ESSENTIAL]);
	});

	it("preselects nothing when no item is essential, so the wizard is never filtered down to an empty list", () => {
		expect(defaultSelectedTags(["greeks"])).toEqual([]);
		expect(defaultSelectedTags([])).toEqual([]);
		expect(defaultSelectedTags(null)).toEqual([]);
	});
});

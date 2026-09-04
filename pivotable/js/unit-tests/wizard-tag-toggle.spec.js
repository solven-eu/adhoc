import { describe, it, expect } from "vitest";

import { toggleTag } from "../src/main/resources/static/ui/js/adhoc-query-wizard-tag-toggle.js";

describe("toggleTag", () => {
	it("selects a tag which was not selected", () => {
		const tags = [];

		expect(toggleTag(tags, "country")).toBe(true);
		expect(tags).toEqual(["country"]);
	});

	it("deselects a tag which was selected", () => {
		const tags = ["country", "currency"];

		expect(toggleTag(tags, "country")).toBe(false);
		expect(tags).toEqual(["currency"]);
	});

	it("mutates in place, as searchOptions is shared by reference across the wizard", () => {
		const tags = ["country"];
		const sameArray = tags;

		toggleTag(tags, "currency");

		expect(sameArray).toBe(tags);
		expect(sameArray).toEqual(["country", "currency"]);
	});

	it("round-trips back to the initial selection", () => {
		const tags = ["country"];

		toggleTag(tags, "currency");
		toggleTag(tags, "currency");

		expect(tags).toEqual(["country"]);
	});

	it("removes only the first occurrence, so a duplicated tag cannot vanish twice over", () => {
		const tags = ["country", "country"];

		expect(toggleTag(tags, "country")).toBe(false);
		expect(tags).toEqual(["country"]);
	});
});

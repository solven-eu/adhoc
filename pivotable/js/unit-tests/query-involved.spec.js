import { describe, it, expect } from "vitest";

import { involvedInQuery, mentionedInError, MIN_MENTION_LENGTH } from "../src/main/resources/static/ui/js/adhoc-query-involved.js";

describe("involvedInQuery", () => {
	it("lists the selected measures and columns", () => {
		const queryModel = {
			selectedMeasures: { delta: true, gamma: true },
			selectedColumns: { country: true },
		};

		expect(involvedInQuery(queryModel)).toEqual({ measures: ["delta", "gamma"], columns: ["country"] });
	});

	it("skips remembered but unselected entries, which are keys set to false", () => {
		const queryModel = {
			selectedMeasures: { delta: true, gamma: false },
			selectedColumns: { country: false },
		};

		expect(involvedInQuery(queryModel)).toEqual({ measures: ["delta"], columns: [] });
	});

	it("tolerates a model carrying neither map", () => {
		expect(involvedInQuery({})).toEqual({ measures: [], columns: [] });
		expect(involvedInQuery(/** @type {any} */ (null))).toEqual({ measures: [], columns: [] });
	});
});

describe("mentionedInError", () => {
	it("spots a measure named in the message", () => {
		expect(mentionedInError("Combinator 'always_throws' failed", "always_throws")).toBe(true);
	});

	it("does not spot a measure absent from the message", () => {
		expect(mentionedInError("Combinator 'always_throws' failed", "delta")).toBe(false);
	});

	it("ignores very short names, which would match almost any message by accident", () => {
		expect("a".length).toBeLessThan(MIN_MENTION_LENGTH);
		expect(mentionedInError("a failure occurred", "a")).toBe(false);
	});

	it("returns false without a message, so a fresh query highlights nothing", () => {
		expect(mentionedInError("", "delta")).toBe(false);
		expect(mentionedInError(null, "delta")).toBe(false);
	});

	// Documents the known limitation: this is a hint for the eye, not an analysis of the error.
	it("matches a name occurring inside a larger word", () => {
		expect(mentionedInError("delta_gamma is unknown", "delta")).toBe(true);
	});
});

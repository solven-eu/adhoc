import { describe, it, expect } from "vitest";

import { keepOnlyMeasure } from "../src/main/resources/static/ui/js/adhoc-query-keep-only-measure.js";

describe("keepOnlyMeasure", () => {
	it("deselects every other selected measure", () => {
		const selectedMeasures = { delta: true, gamma: true, vega: true };

		keepOnlyMeasure(selectedMeasures, "gamma");

		expect(selectedMeasures).toEqual({ delta: false, gamma: true, vega: false });
	});

	it("returns the measures it deselected", () => {
		const selectedMeasures = { delta: true, gamma: true, vega: true };

		expect(keepOnlyMeasure(selectedMeasures, "gamma")).toEqual(["delta", "vega"]);
	});

	it("leaves already-unselected measures untouched, so it only ever narrows the selection", () => {
		const selectedMeasures = { delta: false, gamma: true, vega: true };

		expect(keepOnlyMeasure(selectedMeasures, "gamma")).toEqual(["vega"]);
		expect(selectedMeasures.delta).toBe(false);
	});

	it("keeps the target selected even when it was not", () => {
		const selectedMeasures = { delta: true, gamma: false };

		keepOnlyMeasure(selectedMeasures, "gamma");

		expect(selectedMeasures).toEqual({ delta: false, gamma: true });
	});

	it("is a no-op when the target is the only selected measure", () => {
		const selectedMeasures = { gamma: true };

		expect(keepOnlyMeasure(selectedMeasures, "gamma")).toEqual([]);
		expect(selectedMeasures).toEqual({ gamma: true });
	});

	it("is idempotent", () => {
		const selectedMeasures = { delta: true, gamma: true };

		keepOnlyMeasure(selectedMeasures, "gamma");
		expect(keepOnlyMeasure(selectedMeasures, "gamma")).toEqual([]);
		expect(selectedMeasures).toEqual({ delta: false, gamma: true });
	});

	it("mutates in place, as selectedMeasures is shared by reference across the SPA", () => {
		const selectedMeasures = { delta: true, gamma: true };
		const sameObject = selectedMeasures;

		keepOnlyMeasure(selectedMeasures, "gamma");

		expect(sameObject).toBe(selectedMeasures);
		expect(sameObject.delta).toBe(false);
	});
});

import { expect, test } from "vitest";

import { TIMING_ORDER, formatTimings } from "@/js/adhoc-query-grid-timings.js";

test("returns null when the timing object is null / undefined", () => {
	expect(formatTimings(null)).toBeNull();
	expect(formatTimings(undefined)).toBeNull();
});

test("returns null when no known stage carries a finite number", () => {
	expect(formatTimings({})).toBeNull();
	expect(formatTimings({ sending: "nope", executing: null })).toBeNull();
});

test("emits known stages in canonical (execution-timeline) order regardless of source order", () => {
	const timing = {
		rendering: 7,
		preparingGrid: 3,
		executing: 44,
		sending: 12,
		downloading: 8,
	};
	const res = formatTimings(timing);
	expect(res.entries.map((e) => e.stage)).toEqual(["sending", "executing", "downloading", "preparingGrid", "rendering"]);
});

test("total is the sum of all entry ms values", () => {
	const res = formatTimings({ sending: 10, executing: 20, downloading: 30 });
	expect(res.total).toBe(60);
});

test("non-numeric fields are skipped, not counted", () => {
	const res = formatTimings({ sending: 10, executing: "boom", downloading: null, rendering: 5 });
	expect(res.entries.map((e) => e.stage)).toEqual(["sending", "rendering"]);
	expect(res.total).toBe(15);
});

test("NaN and Infinity are rejected (Number.isFinite guard)", () => {
	expect(formatTimings({ sending: NaN, executing: Infinity })).toBeNull();
});

test("unknown stages are appended AFTER known ones in insertion order", () => {
	const timing = { rendering: 5, futureStage: 11, anotherFuture: 2, sending: 1 };
	const res = formatTimings(timing);
	expect(res.entries.map((e) => e.stage)).toEqual(["sending", "rendering", "futureStage", "anotherFuture"]);
});

test("TIMING_ORDER exposes the canonical ordering used by the UI", () => {
	expect(TIMING_ORDER).toEqual(["sending", "executing", "downloading", "preparingGrid", "sorting", "rowSpanning", "rendering"]);
});

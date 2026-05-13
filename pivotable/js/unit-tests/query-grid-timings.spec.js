// @ts-check
import { expect, test } from "vitest";

import { TIMING_ORDER, formatTimings, hasActiveTiming, finalizeActiveStages } from "@/js/adhoc-query-grid-timings.js";

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

// Live / in-flight stages ---------------------------------------------------------------

test("in-flight stage (has _startedAt, no final ms) yields a growing entry with active=true", () => {
	// startedAt = 1000 ms, now = 1700 ms → elapsed = 700. Downstream UI displays this as
	// `executing=700ms…` until the final duration is written.
	const res = formatTimings({ executing_startedAt: 1000 }, 1700);
	expect(res.entries).toEqual([{ stage: "executing", ms: 700, active: true }]);
	expect(res.anyActive).toBe(true);
});

test("final ms takes precedence over _startedAt (race guard when both are still in the map)", () => {
	// If a finishing step writes the final duration before the cleanup of _startedAt runs,
	// the UI must NOT double-count it — the finite number wins.
	const res = formatTimings({ executing: 42, executing_startedAt: 0 }, 10000);
	expect(res.entries).toEqual([{ stage: "executing", ms: 42, active: false }]);
	expect(res.anyActive).toBe(false);
});

test("mixed finished + in-flight stages land in canonical order with active flags set correctly", () => {
	const res = formatTimings({ sending: 5, executing_startedAt: 100, preparingGrid: 7 }, 250);
	expect(res.entries.map((e) => ({ stage: e.stage, ms: e.ms, active: e.active }))).toEqual([
		{ stage: "sending", ms: 5, active: false },
		{ stage: "executing", ms: 150, active: true },
		{ stage: "preparingGrid", ms: 7, active: false },
	]);
	expect(res.total).toBe(5 + 150 + 7);
	expect(res.anyActive).toBe(true);
});

test("_startedAt sibling keys are consumed, never rendered as their own entry", () => {
	// Regression: without the filter, `executing_startedAt` would show up as a stage with
	// that literal name in the unknown-keys sweep.
	const res = formatTimings({ sending: 1, executing_startedAt: 0 }, 100);
	expect(res.entries.some((e) => e.stage === "executing_startedAt")).toBe(false);
});

test("hasActiveTiming: false for empty / all-finished / null inputs", () => {
	expect(hasActiveTiming(null)).toBe(false);
	expect(hasActiveTiming({})).toBe(false);
	expect(hasActiveTiming({ sending: 5, executing: 7 })).toBe(false);
	expect(hasActiveTiming({ sending: 5, executing: 7, executing_startedAt: 0 })).toBe(false);
});

test("hasActiveTiming: true when at least one _startedAt has no matching finished ms", () => {
	expect(hasActiveTiming({ executing_startedAt: 100 })).toBe(true);
	expect(hasActiveTiming({ sending: 3, rendering_startedAt: 200 })).toBe(true);
});

// ---------------------------------------------------------------------------------------------
// finalizeActiveStages — sweep dangling `_startedAt` markers from an aborted query.
//
// Regression: when the executor's POST throws (e.g. backend down), the `catch` block surfaces
// an error to the user but leaves `timing.sending_startedAt` in place. The renderer's
// hasActiveTiming + formatTimings then treat the stage as in-flight forever, growing the
// "sending" bar on every tick until the next successful query overwrites the bag.
// ---------------------------------------------------------------------------------------------
test("finalizeActiveStages: no-op on null/undefined", () => {
	// Should not throw — just return.
	expect(() => finalizeActiveStages(null)).not.toThrow();
	expect(() => finalizeActiveStages(undefined)).not.toThrow();
});

test("finalizeActiveStages: an in-flight stage is finalized to (now - startedAt) and the marker is dropped", () => {
	const startedAt = new Date(1_000_000);
	const now = 1_000_500;
	const timing = { sending_startedAt: startedAt };
	finalizeActiveStages(timing, now);
	// Stage value now reflects time-until-failure: 500 ms.
	expect(timing.sending).toBe(500);
	// Start marker gone — `hasActiveTiming` will return false from now on.
	expect(timing).not.toHaveProperty("sending_startedAt");
	expect(hasActiveTiming(timing)).toBe(false);
});

test("finalizeActiveStages: stages already finished are not overwritten — only the start marker is dropped", () => {
	const timing = {
		executing: 42,
		executing_startedAt: new Date(0),
	};
	finalizeActiveStages(timing, 5_000);
	expect(timing.executing).toBe(42); // preserved
	expect(timing).not.toHaveProperty("executing_startedAt");
});

test("finalizeActiveStages: multiple dangling stages are all finalized in one pass", () => {
	const now = 2_000;
	const timing = {
		sending_startedAt: new Date(1_000),
		executing_startedAt: new Date(1_500),
		// One already-finished stage interleaved — must stay untouched.
		downloading: 300,
		downloading_startedAt: new Date(0),
	};
	finalizeActiveStages(timing, now);
	expect(timing.sending).toBe(1_000);
	expect(timing.executing).toBe(500);
	expect(timing.downloading).toBe(300); // preserved
	expect(timing).not.toHaveProperty("sending_startedAt");
	expect(timing).not.toHaveProperty("executing_startedAt");
	expect(timing).not.toHaveProperty("downloading_startedAt");
});

test("finalizeActiveStages: negative deltas (clock skew / stale startedAt) clamp to 0", () => {
	const timing = { sending_startedAt: new Date(2_000) };
	finalizeActiveStages(timing, 1_000); // now < startedAt
	expect(timing.sending).toBe(0);
});

test("finalizeActiveStages: accepts a numeric `now`", () => {
	const timing = { sending_startedAt: 0 };
	finalizeActiveStages(timing, 250);
	expect(timing.sending).toBe(250);
});

test("finalizeActiveStages: defaults `now` to Date.now() when omitted", () => {
	// startedAt slightly in the past so a tiny positive delta is guaranteed.
	const timing = { sending_startedAt: new Date(Date.now() - 10) };
	finalizeActiveStages(timing);
	expect(timing.sending).toBeGreaterThanOrEqual(0);
	expect(timing).not.toHaveProperty("sending_startedAt");
});

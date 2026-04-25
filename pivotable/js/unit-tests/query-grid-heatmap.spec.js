import { expect, test } from "vitest";

import { computeMeasureStats, heatmapColor } from "@/js/adhoc-query-grid-heatmap.js";

const gridHelper = { computeMeasureStats, heatmapColor };

// computeMeasureStats -------------------------------------------------------------------

test("computeMeasureStats: tracks min / max / sum / count per measure for numeric values", () => {
	const stats = gridHelper.computeMeasureStats(
		["m1", "m2"],
		[
			{ m1: 1, m2: 10 },
			{ m1: 2, m2: 20 },
			{ m1: 3, m2: 30 },
		],
	);
	expect(stats.m1).toMatchObject({ min: 1, max: 3, sum: 6, count: 3, mean: 2, nullCount: 0, nonNumericCount: 0 });
	expect(stats.m2).toMatchObject({ min: 10, max: 30, sum: 60, count: 3, mean: 20, nullCount: 0, nonNumericCount: 0 });
});

test("computeMeasureStats: bucket nulls and non-numerics separately, keep numeric stats intact", () => {
	const stats = gridHelper.computeMeasureStats(["m"], [{ m: 1 }, { m: "oops" }, { m: null }, { m: NaN }, { m: 5 }]);
	expect(stats.m).toMatchObject({ min: 1, max: 5, sum: 6, count: 2, nullCount: 1, nonNumericCount: 2 });
});

test("computeMeasureStats: empty input produces zeroed stats with nulls for min/max", () => {
	const stats = gridHelper.computeMeasureStats(["m"], []);
	expect(stats.m).toMatchObject({ min: null, max: null, sum: 0, count: 0, mean: 0, variance: 0, nullCount: 0, nonNumericCount: 0 });
});

test("computeMeasureStats: handles negative and mixed-sign values", () => {
	const stats = gridHelper.computeMeasureStats(["m"], [{ m: -5 }, { m: -1 }, { m: 3 }]);
	expect(stats.m).toMatchObject({ min: -5, max: 3, sum: -3, count: 3, mean: -1 });
});

test("computeMeasureStats: variance is computed via Welford and matches the population formula", () => {
	// Values 2,4,4,4,5,5,7,9 → mean = 5, population variance = 4.
	const stats = gridHelper.computeMeasureStats(
		["m"],
		[2, 4, 4, 4, 5, 5, 7, 9].map((v) => ({ m: v })),
	);
	expect(stats.m.mean).toBe(5);
	expect(stats.m.variance).toBeCloseTo(4, 10);
});

// heatmapColor --------------------------------------------------------------------------

test("heatmapColor: null when stats is missing or degenerate (single value, zero range)", () => {
	expect(gridHelper.heatmapColor(1, null)).toBeNull();
	expect(gridHelper.heatmapColor(1, { min: 1, max: 1 })).toBeNull();
	expect(gridHelper.heatmapColor(0, { min: 0, max: 0 })).toBeNull();
});

test("heatmapColor: positive values in an all-positive range — max lands at green extreme", () => {
	// Range [0, 10]: midpoint = 5; max = 10 should be full green (alpha 0.5).
	const color = gridHelper.heatmapColor(10, { min: 0, max: 10 });
	expect(color).toMatch(/rgba\(40,\s*167,\s*69,\s*0?\.5\)/);
});

test("heatmapColor: negative values in a mixed-sign range — min lands at red extreme", () => {
	const color = gridHelper.heatmapColor(-10, { min: -10, max: 10 });
	expect(color).toMatch(/rgba\(220,\s*53,\s*69,\s*0?\.5\)/);
});

test("heatmapColor: alpha is capped at 0.5 even at the extremes", () => {
	// The formatter caps at 0.5 so max-intensity cells stay legible.
	const color = gridHelper.heatmapColor(1000, { min: -1000, max: 1000 });
	const match = color.match(/rgba\(\d+,\s*\d+,\s*\d+,\s*([\d.]+)\)/);
	expect(match).not.toBeNull();
	expect(parseFloat(match[1])).toBeLessThanOrEqual(0.5);
});

test("heatmapColor: zero value in a mixed-sign range is uncolored (null)", () => {
	// In a mixed-sign range, zero is the neutral centre and yields no color.
	expect(gridHelper.heatmapColor(0, { min: -10, max: 10 })).toBeNull();
});

test("heatmapColor: tightly clustered all-positive column gets full contrast across [min, max]", () => {
	// Regression: under the old algorithm (normalised by max abs), a column with values
	// near 1_000 was almost mono-coloured because every value normalised to ~1. Now the
	// column's midpoint is the neutral centre, so min and max both reach full intensity.
	const stats = { min: 1000, max: 1010 };
	const minColor = gridHelper.heatmapColor(1000, stats);
	const maxColor = gridHelper.heatmapColor(1010, stats);
	const midColor = gridHelper.heatmapColor(1005, stats);
	expect(minColor).toMatch(/rgba\(220,\s*53,\s*69,\s*0?\.5\)/);
	expect(maxColor).toMatch(/rgba\(40,\s*167,\s*69,\s*0?\.5\)/);
	// Midpoint is the neutral centre — uncolored.
	expect(midColor).toBeNull();
});

test("heatmapColor: midpoint of a mixed-sign asymmetric range is zero, not the arithmetic mean", () => {
	// Range [-1, 9]: zero is still the divider between red and green (negative = red).
	expect(gridHelper.heatmapColor(0, { min: -1, max: 9 })).toBeNull();
	// -1 reaches full red intensity.
	expect(gridHelper.heatmapColor(-1, { min: -1, max: 9 })).toMatch(/rgba\(220,\s*53,\s*69,\s*0?\.5\)/);
	// 9 reaches full green intensity.
	expect(gridHelper.heatmapColor(9, { min: -1, max: 9 })).toMatch(/rgba\(40,\s*167,\s*69,\s*0?\.5\)/);
});

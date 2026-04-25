// Heatmap helpers for the query grid's measure columns.
//
// Kept in its own file because `adhoc-query-grid-helper.js` imports SlickGrid / Sortable /
// lodash / bootstrap — which the Vitest jsdom environment cannot resolve — and both of the
// helpers exported below are pure data functions. Extracting them here means they are
// directly unit-testable (see `unit-tests/query-grid-heatmap.spec.js`) and keeps the grid
// helper focused on DOM-binding concerns.

// Per-measure aggregates over the cells found in `values`. Used by the heatmap formatter
// (min/max drive the color scale), by `updateFooters` (min/max in the footer), and by
// the per-column Statistics modal (mean / variance / null-count / non-numeric-count).
//
// Numeric values feed the descriptive statistics. Non-numeric cells are bucketed:
//   - `nullCount`: cells that are `null` or `undefined`.
//   - `nonNumericCount`: cells that are neither numeric nor null/undefined (e.g. a String
//     surfaced because the cell is an error message or a non-aggregatable value).
// Variance uses Welford's online algorithm so the result is numerically stable for both
// tiny ranges (`min == max`) and large-magnitude data — the naive `E(X²) − E(X)²` form
// loses precision when the values cluster far from zero.
export const computeMeasureStats = function (measureNames, values) {
	const stats = {};
	for (const measureName of measureNames) {
		// `mean`/`m2` are Welford accumulators; `mean` is the running mean, `m2` is the
		// sum of squared deviations from the running mean. Variance is `m2 / count`.
		stats[measureName] = {
			min: null,
			max: null,
			sum: 0,
			count: 0,
			mean: 0,
			m2: 0,
			nullCount: 0,
			nonNumericCount: 0,
		};
	}
	for (let rowIndex = 0; rowIndex < values.length; rowIndex++) {
		const row = values[rowIndex];
		for (const measureName of measureNames) {
			const v = row[measureName];
			const s = stats[measureName];
			if (v === null || v === undefined) {
				s.nullCount += 1;
				continue;
			}
			if (typeof v !== "number" || Number.isNaN(v)) {
				s.nonNumericCount += 1;
				continue;
			}
			s.sum += v;
			s.count += 1;
			s.min = s.min === null ? v : Math.min(s.min, v);
			s.max = s.max === null ? v : Math.max(s.max, v);
			// Welford update.
			const delta = v - s.mean;
			s.mean += delta / s.count;
			s.m2 += delta * (v - s.mean);
		}
	}
	// Materialise variance from m2 (population variance, count divisor — matches the
	// usual "describe" output and stays defined for count == 1).
	for (const measureName of measureNames) {
		const s = stats[measureName];
		s.variance = s.count > 0 ? s.m2 / s.count : 0;
	}
	return stats;
};

// Map a numeric value to a heatmap background color based on the measure's OBSERVED
// [min, max] range. The scale is calibrated per column so that contrast spans the full
// alpha range from the column's minimum to its maximum, regardless of where those
// endpoints sit on the number line.
//
// Earlier version normalised by max(|min|, |max|), so a column whose values were all
// clustered near 1_000 (and far from 0) ended up nearly mono-coloured because every
// value mapped to a normalised position close to 1. Now we map linearly:
//   - the minimum value lands at one end of the palette (lowest = red),
//   - the maximum value lands at the other end (highest = green),
//   - values in between get an intermediate alpha proportional to their position in
//     the column's range.
//
// The midpoint behaviour depends on whether the column straddles zero:
//   - mixed-sign range (min < 0 < max): zero stays the neutral centre — values below 0
//     get red intensity = |v / min|, values above 0 get green intensity = v / max. This
//     preserves the "negative = red, positive = green" convention.
//   - all-positive (or all-negative) range: the column's MIDPOINT becomes the neutral
//     centre. Values below the midpoint shade red (lowest = full red), values above
//     shade green (highest = full green). This is what makes a tightly-clustered
//     all-positive column readable: the contrast is allocated to the column's actual
//     spread, not wasted on the (nominally large) distance from zero.
//
// Degenerate ranges (single distinct value, count of 1) return null so the cell stays
// uncolored — the user gets no false signal of variation.
export const heatmapColor = function (value, stats) {
	if (!stats || stats.min === stats.max) {
		return null;
	}
	const { min, max } = stats;
	// Cap alpha at 0.5 so the digits stay readable even at the extremes.
	const ALPHA_CAP = 0.5;

	const colorFromIntensity = function (intensity, positive) {
		const alpha = Math.max(0, Math.min(ALPHA_CAP, intensity * ALPHA_CAP));
		if (alpha === 0) return null;
		return positive ? `rgba(40, 167, 69, ${alpha})` : `rgba(220, 53, 69, ${alpha})`;
	};

	if (min < 0 && max > 0) {
		// Mixed-sign range: keep zero as the neutral centre. Each side scales independently
		// so the most-extreme negative is full red and the most-extreme positive is full green.
		if (value > 0) {
			return colorFromIntensity(value / max, true);
		}
		if (value < 0) {
			return colorFromIntensity(value / min, false);
		}
		return null;
	}

	// All values share a sign (or include zero on one boundary). Use the column's
	// midpoint as the neutral centre and stretch the gradient across [min, max].
	const mid = (min + max) / 2;
	if (value === mid) {
		return null;
	}
	if (value > mid) {
		return colorFromIntensity((value - mid) / (max - mid), true);
	}
	return colorFromIntensity((mid - value) / (mid - min), false);
};

// Pure formatting helpers for the per-stage timings emitted by the executor and grid hooks.
// Extracted from `adhoc-query-grid.js` so the logic can be unit-tested without a DOM, and so
// the main grid file stays focused on SlickGrid lifecycle.
//
// The raw `tabularView.timing` object shape — { sending, executing, downloading, preparingGrid,
// sorting, rowSpanning, rendering, … } — is produced by:
//   - adhoc-query-executor.js: sending / executing / downloading
//   - adhoc-query-grid.js    : preparingGrid / sorting / rowSpanning / rendering
// Values are integers in milliseconds.

// Display order — stages appear in the UI in the same order they fire at runtime, so the line
// reads left-to-right like an execution timeline. Unknown keys are appended after in insertion
// order to stay forward-compatible with new stages.
export const TIMING_ORDER = ["sending", "executing", "downloading", "preparingGrid", "sorting", "rowSpanning", "rendering"];

// Turn the raw timing object into a { entries, total } shape ready for template rendering, or
// null when there is nothing to show (no timing attached yet, or all fields are non-numeric).
// `entries` is `[{ stage, ms }]` in display order; `total` is the sum of all `ms` values.
export const formatTimings = function (timing) {
	if (!timing) return null;

	const entries = [];
	let total = 0;

	for (const stage of TIMING_ORDER) {
		const ms = timing[stage];
		if (typeof ms === "number" && Number.isFinite(ms)) {
			entries.push({ stage, ms });
			total += ms;
		}
	}
	// Any unknown keys get appended after in insertion order — keeps forward-compat when the
	// executor starts reporting new stages without needing a UI change.
	for (const stage of Object.keys(timing)) {
		if (TIMING_ORDER.includes(stage)) continue;
		const ms = timing[stage];
		if (typeof ms === "number" && Number.isFinite(ms)) {
			entries.push({ stage, ms });
			total += ms;
		}
	}

	if (entries.length === 0) return null;
	return { entries, total };
};

export default { TIMING_ORDER, formatTimings };

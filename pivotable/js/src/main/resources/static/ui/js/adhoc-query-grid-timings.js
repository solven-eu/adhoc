// Pure formatting helpers for the per-stage timings emitted by the executor and grid hooks.
// Extracted from `adhoc-query-grid.js` so the logic can be unit-tested without a DOM, and so
// the main grid file stays focused on SlickGrid lifecycle.
//
// The raw `tabularView.timing` object shape — { sending, executing, downloading, preparingGrid,
// sorting, rowSpanning, rendering, … } — is produced by:
//   - adhoc-query-executor.js: sending / executing / downloading
//   - adhoc-query-grid.js    : preparingGrid / sorting / rowSpanning / rendering
// Values are integers in milliseconds.
//
// An IN-FLIGHT step is indicated by a sibling key `<stage>_startedAt` holding a Date. When that
// key is present and `<stage>` is not yet a number, the step is active — the renderer computes
// a live duration `now - startedAt` instead of waiting for the final value. Callers tick a
// `now` reference on a short interval to drive the live-update.

// Display order — stages appear in the UI in the same order they fire at runtime, so the line
// reads left-to-right like an execution timeline. Unknown keys are appended after in insertion
// order to stay forward-compatible with new stages.
export const TIMING_ORDER = ["sending", "executing", "downloading", "preparingGrid", "sorting", "rowSpanning", "rendering"];

// Internal: extract the display entry (or null) for one stage given the raw timing map.
// A stage yields an entry when either:
//   - `timing[stage]` is a finite number (step is finished — show that duration, `active` false).
//   - `timing[stage + '_startedAt']` is a Date or numeric ms (step is in-flight — show
//     `now - startedAt`, `active` true).
// Otherwise returns null so the stage stays hidden until it starts.
const entryFor = function (timing, stage, now) {
	const ms = timing[stage];
	if (typeof ms === "number" && Number.isFinite(ms)) {
		return { stage, ms, active: false };
	}
	const startedAt = timing[stage + "_startedAt"];
	if (startedAt) {
		// Date - Date returns ms; Date - number also returns ms when `now` is given as epoch ms.
		// Guard against the rare case where `startedAt` is a number by normalising both sides.
		const startMs = typeof startedAt === "number" ? startedAt : startedAt.getTime ? startedAt.getTime() : +startedAt;
		const nowMs = typeof now === "number" ? now : now && now.getTime ? now.getTime() : +new Date();
		return { stage, ms: Math.max(0, nowMs - startMs), active: true };
	}
	return null;
};

// Turn the raw timing object into a { entries, total, anyActive } shape ready for template
// rendering, or null when there is nothing to show (no timing attached yet, and no stage is
// in-flight). `entries` is `[{ stage, ms, active }]` in display order; `total` is the sum of
// all `ms` values; `anyActive` flips the rendering to "this query is still running".
//
// The optional `now` argument (Date or ms number) lets the caller drive live-updating of active
// stages. Defaults to "right now" at call time.
export const formatTimings = function (timing, now) {
	if (!timing) return null;

	const entries = [];
	let total = 0;
	let anyActive = false;

	for (const stage of TIMING_ORDER) {
		const entry = entryFor(timing, stage, now);
		if (entry) {
			entries.push(entry);
			total += entry.ms;
			if (entry.active) anyActive = true;
		}
	}
	// Any unknown keys get appended after in insertion order — keeps forward-compat when the
	// executor starts reporting new stages without needing a UI change. Skip `_startedAt`
	// sibling keys (they are consumed via `entryFor` above, not rendered themselves).
	for (const stage of Object.keys(timing)) {
		if (TIMING_ORDER.includes(stage) || stage.endsWith("_startedAt")) continue;
		const entry = entryFor(timing, stage, now);
		if (entry) {
			entries.push(entry);
			total += entry.ms;
			if (entry.active) anyActive = true;
		}
	}

	if (entries.length === 0) return null;
	return { entries, total, anyActive };
};

// True when `timing` has at least one `<stage>_startedAt` key whose matching `<stage>` is not yet
// a finite number — i.e. the view is mid-query and the timings bar should tick.
export const hasActiveTiming = function (timing) {
	if (!timing) return false;
	for (const key of Object.keys(timing)) {
		if (!key.endsWith("_startedAt")) continue;
		const stage = key.slice(0, -"_startedAt".length);
		const ms = timing[stage];
		if (!(typeof ms === "number" && Number.isFinite(ms))) {
			return true;
		}
	}
	return false;
};

export default { TIMING_ORDER, formatTimings, hasActiveTiming };

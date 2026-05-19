// @ts-check

// Format a duration (ms) as a compact human-readable age suffix, e.g. "3s ago", "12m ago",
// "2h ago", "4d ago". Returns the empty string when the input is not a positive finite number,
// which keeps the caller's template simple (`v-if="ageText"`).
//
// Buckets are intentionally coarse: the view-age indicator is a "how stale is this snapshot"
// hint, not a stopwatch — the live performance digits already carry sub-second precision
// during query execution.
/**
 * @param {number | null | undefined} ms
 * @returns {string}
 */
export function formatViewAge(ms) {
	if (!Number.isFinite(/** @type {number} */ (ms)) || /** @type {number} */ (ms) < 0) return "";
	const millis = /** @type {number} */ (ms);
	if (millis < 1000) return "just now";
	const seconds = Math.floor(millis / 1000);
	// Buckets are wider than calendar units: we stay in seconds up to 120s, in minutes up to
	// 120 min, in hours up to 48h. Crossing a boundary on the wider unit (e.g. "121s ago" →
	// "2m ago") feels more natural than flipping every 60.
	if (seconds < 120) return seconds + "s ago";
	const minutes = Math.floor(seconds / 60);
	if (minutes < 120) return minutes + "m ago";
	const hours = Math.floor(minutes / 60);
	if (hours < 48) return hours + "h ago";
	const days = Math.floor(hours / 24);
	return days + "d ago";
}

// Pick a tick cadence (ms) appropriate for the current age. Younger ages tick at 1s so the
// "Xs ago" digits feel live; older ages tick every minute since the bucket only changes that
// often. Keeps the overall CPU cost negligible regardless of how long the user keeps the page
// open.
/**
 * @param {number} ageMs
 * @returns {number}
 */
export function pickAgeTickMs(ageMs) {
	// Aligned with formatViewAge's buckets: seconds (<120s) → 1s tick; minutes (<120 min) →
	// 30s tick; hours (<48h) → 60s tick; days → hourly (bucket only flips once a day).
	if (ageMs < 120_000) return 1_000;
	if (ageMs < 7_200_000) return 30_000;
	if (ageMs < 172_800_000) return 60_000;
	return 3_600_000;
}
